use futures::{TryFutureExt, TryStreamExt};
use money2::{Exchange, HistoricalExchangeRates};
use sqlx::{Pool, Postgres, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{DepartmentColumns, JobColumns, JobDepartmentColumns, LocationColumns, OrganizationColumns},
	Retrievable,
	WriteWhereClause,
};
use winvoice_match::MatchJob;
use winvoice_schema::Job;

use super::PgJob;
use crate::{
	fmt::PgLocationRecursiveCte,
	schema::{util, PgLocation},
	PgSchema,
};

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgJob
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Job;
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchJob;

	/// Retrieve all [`Job`]s (via `connection`) that match the `match_condition`.
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(connection: &Pool<Postgres>, match_condition: Self::Match) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: JobColumns = JobColumns::default();
		const DEPARTMENTS_AGGREGATED_IDENT: &str = "departments_aggregated";
		const ORGANIZATION_COLUMNS_UNIQUE: OrganizationColumns = OrganizationColumns::unique();

		let columns = COLUMNS.default_scope();
		let department_columns = DepartmentColumns::default().default_scope();
		let exchange_rates_fut = HistoricalExchangeRates::try_index(None).map_err(util::finance_err_to_sqlx);
		let job_department_columns = JobDepartmentColumns::default().default_scope();
		let match_location = match_condition.client.location.clone();
		let mut query = PgLocation::query_with_recursive(&match_location);
		let organization_columns = OrganizationColumns::default().default_scope();

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_more_columns(&organization_columns.r#as(ORGANIZATION_COLUMNS_UNIQUE))
			.push(", array_agg((")
			.push_columns(&department_columns)
			.push("))")
			.push(sql::AS)
			.push(DEPARTMENTS_AGGREGATED_IDENT)
			.push_default_from::<JobColumns>()
			.push_default_equijoin::<OrganizationColumns, _, _>(organization_columns.id, columns.client_id)
			.push(sql::LEFT)
			.push_default_equijoin::<JobDepartmentColumns, _, _>(job_department_columns.job_id, columns.id)
			.push(sql::LEFT)
			.push_default_equijoin::<DepartmentColumns, _, _>(
				department_columns.id,
				job_department_columns.department_id,
			)
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_location),
				LocationColumns::DEFAULT_ALIAS,
				LocationColumns::default().default_scope().id,
				organization_columns.location_id,
			);

		let exchanged_condition =
			exchange_rates_fut.await.map(|rates| match_condition.exchange(Default::default(), &rates))?;

		PgSchema::write_where_clause(
			PgSchema::write_where_clause(
				PgSchema::write_where_clause(
					Default::default(),
					JobColumns::DEFAULT_ALIAS,
					&exchanged_condition,
					&mut query,
				),
				OrganizationColumns::DEFAULT_ALIAS,
				&exchanged_condition.client,
				&mut query,
			),
			DepartmentColumns::DEFAULT_ALIAS,
			&exchanged_condition.departments,
			&mut query,
		);

		query.push(sql::GROUP_BY).separated(',').push(columns.id).push(organization_columns.id);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move {
				Self::row_to_view(connection, COLUMNS, DEPARTMENTS_AGGREGATED_IDENT, ORGANIZATION_COLUMNS_UNIQUE, &row)
					.await
			})
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;
	use std::collections::HashSet;

	use futures::{stream, StreamExt};
	use mockd::{address, company, words};
	use money2::{Exchange, HistoricalExchangeRates};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{DepartmentAdapter, JobAdapter, LocationAdapter, OrganizationAdapter},
		Retrievable,
	};
	use winvoice_match::{Match, MatchDepartment, MatchInvoice, MatchJob, MatchStr};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Currency,
		Invoice,
		InvoiceDate,
		Money,
	};

	use crate::schema::{util, PgDepartment, PgJob, PgLocation, PgOrganization};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::city(), None).await.unwrap();

		let street = PgLocation::create(&connection, None, util::rand_street_name(), city.into()).await.unwrap();

		let (department, department2, location, location2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
		)
		.unwrap();

		let (organization, organization2) = futures::try_join!(
			PgOrganization::create(&connection, location, company::company()),
			PgOrganization::create(&connection, location2, company::company()),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let job = PgJob::create(
			&mut tx,
			organization.clone(),
			Utc.with_ymd_and_hms(1990, 08, 01, 09, 00, 00).latest(),
			Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
			[&department, &department2].into_iter().cloned().collect(),
			Duration::from_secs(300),
			Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let job2 = PgJob::create(
			&mut tx,
			organization2.clone(),
			Utc.with_ymd_and_hms(3000, 01, 16, 10, 00, 00).latest(),
			Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
			[&department2].into_iter().cloned().collect(),
			Duration::from_secs(900),
			Invoice {
				date: InvoiceDate { issued: Utc.with_ymd_and_hms(3000, 01, 17, 12, 30, 00).unwrap(), paid: None }
					.into(),
				hourly_rate: Money::new(299_99, 2, Currency::Jpy),
			},
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let job3 = PgJob::create(
			&mut tx,
			organization.clone(),
			Utc.with_ymd_and_hms(2011, 03, 17, 13, 07, 07).latest(),
			Utc.with_ymd_and_hms(2011, 03, 17, 12, 07, 07).unwrap(),
			[&department].into_iter().cloned().collect(),
			Duration::from_secs(900),
			Invoice {
				date: InvoiceDate {
					issued: Utc.with_ymd_and_hms(2011, 03, 18, 08, 00, 00).unwrap(),
					paid: Utc.with_ymd_and_hms(2011, 03, 19, 17, 00, 00).latest(),
				}
				.into(),
				hourly_rate: Money::new(20_00, 2, Currency::Eur),
			},
			String::new(),
			"Do something".into(),
		)
		.await
		.unwrap();

		let job4 = PgJob::create(
			&mut tx,
			organization2.clone(),
			None,
			Utc.with_ymd_and_hms(2022, 01, 02, 01, 01, 01).unwrap(),
			Default::default(),
			Duration::from_secs(900),
			Invoice { date: None, hourly_rate: Money::new(200_00, 2, Currency::Nok) },
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();

		assert_eq!(PgJob::retrieve(&connection, job.id.into()).await.unwrap().as_slice(), &[job
			.clone()
			.exchange(Default::default(), &HistoricalExchangeRates::index(Some(job.date_open.into())).await)]);

		assert_eq!(
			PgJob::retrieve(&connection, MatchJob {
				departments: [].into_iter().collect(),
				id: Match::from(job2.id) | job3.id.into(),
				invoice: MatchInvoice {
					date_issued: Some(Match::Any).into(),
					hourly_rate: [job2.invoice.hourly_rate, job3.invoice.hourly_rate].into_iter().collect(),
					..Default::default()
				},
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			stream::iter([&job2, &job3])
				.then(|j| async {
					j.clone()
						.exchange(Default::default(), &HistoricalExchangeRates::index(Some(j.date_open.into())).await)
				})
				.collect::<HashSet<_>>()
				.await,
		);

		assert_eq!(
			PgJob::retrieve(&connection, MatchJob {
				id: Match::from(job.id) | job4.id.into(),
				invoice: MatchInvoice { date_issued: None.into(), ..Default::default() },
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			stream::iter([&job, &job4])
				.then(|j| async {
					j.clone()
						.exchange(Default::default(), &HistoricalExchangeRates::index(Some(j.date_open.into())).await)
				})
				.collect::<HashSet<_>>()
				.await,
		);

		assert_eq!(
			PgJob::retrieve(&connection, MatchJob {
				departments: MatchDepartment {
					name: MatchStr::Or(job.departments.iter().map(|d| d.name.clone().into()).collect()),
					..Default::default()
				}
				.into(),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			stream::iter([&job, &job2, &job3])
				.then(|j| async {
					j.clone()
						.exchange(Default::default(), &HistoricalExchangeRates::index(Some(j.date_open.into())).await)
				})
				.collect::<HashSet<_>>()
				.await,
		);
	}
}
