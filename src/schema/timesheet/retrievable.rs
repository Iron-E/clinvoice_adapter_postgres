use futures::{TryFutureExt, TryStreamExt};
use money2::{Exchange, ExchangeRates};
use sqlx::{Pool, Postgres, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{
		EmployeeColumns,
		ExpenseColumns,
		JobColumns,
		LocationColumns,
		OrganizationColumns,
		TimesheetColumns,
	},
	Retrievable,
	WriteWhereClause,
};
use winvoice_match::MatchTimesheet;
use winvoice_schema::Timesheet;

use super::PgTimesheet;
use crate::{
	fmt::PgLocationRecursiveCte,
	schema::{util, PgLocation},
	PgSchema,
};

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgTimesheet
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Timesheet;
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchTimesheet;

	/// Retrieve all [`Timesheet`]s (via `connection`) that match the `match_condition`.
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: TimesheetColumns<&str> = TimesheetColumns::default();

		const EXPENSES_AGGREGATED_IDENT: &str = "expenses_aggregated";

		const EMPLOYEE_COLUMNS_UNIQUE: EmployeeColumns<&str> = EmployeeColumns::unique();
		const JOB_COLUMNS_UNIQUE: JobColumns<&str> = JobColumns::unique();
		const ORGANIZATION_COLUMNS_UNIQUE: OrganizationColumns<&str> =
			OrganizationColumns::unique();

		let columns = COLUMNS.default_scope();
		let employee_columns = EmployeeColumns::default().default_scope();
		let exchange_rates_fut = ExchangeRates::new().map_err(util::finance_err_to_sqlx);
		let expense_columns = ExpenseColumns::default().default_scope();
		let job_columns = JobColumns::default().default_scope();
		let location_columns = LocationColumns::default().default_scope();
		let match_location = match_condition.job.client.location.clone();
		let mut query = PgLocation::query_with_recursive(&match_location);
		let organization_columns = OrganizationColumns::default().default_scope();

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_more_columns(&employee_columns.r#as(EMPLOYEE_COLUMNS_UNIQUE))
			.push(",array_agg((")
			.push_columns(&expense_columns)
			.push("))")
			.push(sql::AS)
			.push(EXPENSES_AGGREGATED_IDENT)
			.push_more_columns(&job_columns.r#as(JOB_COLUMNS_UNIQUE))
			.push_more_columns(&organization_columns.r#as(ORGANIZATION_COLUMNS_UNIQUE))
			.push_default_from::<TimesheetColumns>()
			.push_default_equijoin::<EmployeeColumns, _, _>(
				employee_columns.id,
				columns.employee_id,
			)
			.push(sql::LEFT)
			.push_default_equijoin::<ExpenseColumns, _, _>(expense_columns.timesheet_id, columns.id)
			.push_default_equijoin::<JobColumns, _, _>(job_columns.id, columns.job_id)
			.push_default_equijoin::<OrganizationColumns, _, _>(
				organization_columns.id,
				job_columns.client_id,
			)
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_location),
				LocationColumns::DEFAULT_ALIAS,
				location_columns.id,
				organization_columns.location_id,
			);

		let exchanged_condition = exchange_rates_fut
			.await
			.map(|rates| match_condition.exchange(Default::default(), &rates))?;

		PgSchema::write_where_clause(
			PgSchema::write_where_clause(
				PgSchema::write_where_clause(
					PgSchema::write_where_clause(
						PgSchema::write_where_clause(
							Default::default(),
							TimesheetColumns::DEFAULT_ALIAS,
							&exchanged_condition,
							&mut query,
						),
						EmployeeColumns::DEFAULT_ALIAS,
						&exchanged_condition.employee,
						&mut query,
					),
					ExpenseColumns::DEFAULT_ALIAS,
					&exchanged_condition.expenses,
					&mut query,
				),
				JobColumns::DEFAULT_ALIAS,
				&exchanged_condition.job,
				&mut query,
			),
			OrganizationColumns::DEFAULT_ALIAS,
			&exchanged_condition.job.client,
			&mut query,
		);

		query
			.push(sql::GROUP_BY)
			.separated(',')
			.push(columns.id)
			.push(employee_columns.id)
			.push(job_columns.id)
			.push(organization_columns.id);

		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move {
				Self::row_to_view(
					connection,
					COLUMNS,
					EMPLOYEE_COLUMNS_UNIQUE,
					EXPENSES_AGGREGATED_IDENT,
					JOB_COLUMNS_UNIQUE,
					ORGANIZATION_COLUMNS_UNIQUE,
					&row,
				)
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

	use money2::{Exchange, ExchangeRates};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{
			EmployeeAdapter,
			JobAdapter,
			LocationAdapter,
			OrganizationAdapter,
			TimesheetAdapter,
		},
		Retrievable,
	};
	use winvoice_match::{Match, MatchSet, MatchTimesheet};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Currency,
		Invoice,
		InvoiceDate,
		Money,
	};

	use crate::schema::{util, PgEmployee, PgJob, PgLocation, PgOrganization, PgTimesheet};

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
			PgOrganization::create(&connection, utah, "Some Other Organizatión".into()),
		)
		.unwrap();

		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(&connection, "My Name".into(), "Employed".into(), "Janitor".into()),
			PgEmployee::create(
				&connection,
				"Another Gúy".into(),
				"Management".into(),
				"Assistant to Regional Manager".into(),
			),
		)
		.unwrap();

		let (job, job2) = futures::try_join!(
			PgJob::create(
				&connection,
				organization.clone(),
				None,
				Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
				Duration::from_secs(900),
				Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization2.clone(),
				Utc.with_ymd_and_hms(3000, 01, 13, 11, 30, 00).latest(),
				Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
				Duration::from_secs(900),
				Invoice {
					date: Some(InvoiceDate {
						issued: Utc.with_ymd_and_hms(3000, 01, 13, 11, 45, 00).unwrap(),
						paid: Utc.with_ymd_and_hms(3000, 01, 15, 14, 27, 00).latest(),
					}),
					hourly_rate: Money::new(200_00, 2, Currency::Jpy),
				},
				String::new(),
				"Do something".into()
			),
		)
		.unwrap();

		// {{{
		let mut transaction = connection.begin().await.unwrap();

		let timesheet = PgTimesheet::create(
			&mut transaction,
			employee,
			Vec::new(),
			job,
			Utc::now(),
			None,
			"My work notes".into(),
		)
		.await
		.unwrap();

		let timesheet2 = PgTimesheet::create(
			&mut transaction,
			employee2,
			vec![(
				"Flight".into(),
				Money::new(300_56, 2, Currency::Usd),
				"Trip to Hawaii for research".into(),
			)],
			job2,
			Utc.with_ymd_and_hms(2022, 06, 08, 15, 27, 00).unwrap(),
			Utc.with_ymd_and_hms(2022, 06, 09, 07, 00, 00).latest(),
			"More work notes".into(),
		)
		.await
		.unwrap();

		transaction.commit().await.unwrap();
		// }}}

		let exchange_rates = ExchangeRates::new().await.unwrap();

		assert_eq!(
			PgTimesheet::retrieve(&connection, MatchTimesheet {
				expenses: MatchSet::Not(MatchSet::Contains(Default::default()).into()),
				employee: Match::Or(vec![
					timesheet.employee.id.into(),
					timesheet2.employee.id.into(),
				])
				.into(),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.as_slice(),
			&[timesheet.exchange(Default::default(), &exchange_rates)],
		);
	}
}
