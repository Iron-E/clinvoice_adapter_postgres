use futures::{TryFutureExt, TryStreamExt};
use money2::{Exchange, ExchangeRates};
use sqlx::{Pool, Postgres, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{JobColumns, LocationColumns, OrganizationColumns},
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
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: JobColumns<&str> = JobColumns::default();

		const ORGANIZATION_COLUMNS_UNIQUE: OrganizationColumns<&str> =
			OrganizationColumns::unique();

		let columns = COLUMNS.default_scope();
		let exchange_rates_fut = ExchangeRates::new().map_err(util::finance_err_to_sqlx);
		let match_location = match_condition.client.location.clone();
		let mut query = PgLocation::query_with_recursive(&match_location);
		let organization_columns = OrganizationColumns::default().default_scope();

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_more_columns(&organization_columns.r#as(ORGANIZATION_COLUMNS_UNIQUE))
			.push_default_from::<JobColumns<char>>()
			.push_default_equijoin::<OrganizationColumns<char>, _, _>(
				organization_columns.id,
				columns.client_id,
			)
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_location),
				LocationColumns::<char>::DEFAULT_ALIAS,
				LocationColumns::default().default_scope().id,
				organization_columns.location_id,
			);

		let exchanged_condition = exchange_rates_fut
			.await
			.map(|rates| match_condition.exchange(Default::default(), &rates))?;

		PgSchema::write_where_clause(
			PgSchema::write_where_clause(
				Default::default(),
				JobColumns::<char>::DEFAULT_ALIAS,
				&exchanged_condition,
				&mut query,
			),
			OrganizationColumns::<char>::DEFAULT_ALIAS,
			&exchanged_condition.client,
			&mut query,
		);

		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move {
				Self::row_to_view(connection, COLUMNS, ORGANIZATION_COLUMNS_UNIQUE, &row).await
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

	use money2::{Exchange, ExchangeRates};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{JobAdapter, LocationAdapter, OrganizationAdapter},
		Retrievable,
	};
	use winvoice_match::{Match, MatchInvoice, MatchJob};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Currency,
		Invoice,
		InvoiceDate,
		Money,
	};

	use crate::schema::{util, PgJob, PgLocation, PgOrganization};

	#[tokio::test]
	async fn retrieve()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None).await.unwrap();

		let usa = PgLocation::create(&connection, "USA".into(), Some(earth)).await.unwrap();

		let (arizona, utah) = futures::try_join!(
			PgLocation::create(&connection, "Arizona".into(), Some(usa.clone())),
			PgLocation::create(&connection, "Utah".into(), Some(usa.clone())),
		)
		.unwrap();

		let (organization, organization2) = futures::try_join!(
			PgOrganization::create(&connection, arizona.clone(), "Some Organization".into()),
			PgOrganization::create(&connection, utah.clone(), "Some Other Organizatión".into()),
		)
		.unwrap();

		let (job, job2, job3, job4) = futures::try_join!(
			PgJob::create(
				&connection,
				organization.clone(),
				Utc.with_ymd_and_hms(1990, 08, 01, 09, 00, 00).latest(),
				Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
				Duration::from_secs(300),
				Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization2.clone(),
				Utc.with_ymd_and_hms(3000, 01, 16, 10, 00, 00).latest(),
				Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
				Duration::from_secs(900),
				Invoice {
					date: Some(InvoiceDate {
						issued: Utc.with_ymd_and_hms(3000, 01, 17, 12, 30, 00).unwrap(),
						paid: None,
					}),
					hourly_rate: Money::new(299_99, 2, Currency::Jpy),
				},
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization.clone(),
				Utc.with_ymd_and_hms(2011, 03, 17, 13, 07, 07).latest(),
				Utc.with_ymd_and_hms(2011, 03, 17, 12, 07, 07).unwrap(),
				Duration::from_secs(900),
				Invoice {
					date: Some(InvoiceDate {
						issued: Utc.with_ymd_and_hms(2011, 03, 18, 08, 00, 00).unwrap(),
						paid: Utc.with_ymd_and_hms(2011, 03, 19, 17, 00, 00).latest(),
					}),
					hourly_rate: Money::new(20_00, 2, Currency::Eur),
				},
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization2.clone(),
				None,
				Utc.with_ymd_and_hms(2022, 01, 02, 01, 01, 01).unwrap(),
				Duration::from_secs(900),
				Invoice { date: None, hourly_rate: Money::new(200_00, 2, Currency::Nok) },
				String::new(),
				"Do something".into()
			),
		)
		.unwrap();

		let exchange_rates = ExchangeRates::new().await.unwrap();

		assert_eq!(PgJob::retrieve(&connection, job.id.into()).await.unwrap().as_slice(), &[job
			.clone()
			.exchange(Default::default(), &exchange_rates)],);

		assert_eq!(
			PgJob::retrieve(&connection, MatchJob {
				id: Match::Or(vec![job2.id.into(), job3.id.into()]),
				invoice: MatchInvoice {
					date_issued: Some(Match::Any).into(),
					..Default::default()
				},
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			[
				job2.exchange(Default::default(), &exchange_rates),
				job3.exchange(Default::default(), &exchange_rates),
			]
			.into_iter()
			.collect::<HashSet<_>>(),
		);

		assert_eq!(
			PgJob::retrieve(&connection, MatchJob {
				id: Match::Or(vec![job.id.into(), job4.id.into()]),
				invoice: MatchInvoice { date_issued: None.into(), ..Default::default() },
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			[
				job.exchange(Default::default(), &exchange_rates),
				job4.exchange(Default::default(), &exchange_rates),
			]
			.into_iter()
			.collect::<HashSet<_>>(),
		);
	}
}
