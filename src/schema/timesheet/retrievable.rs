use futures::{TryFutureExt, TryStreamExt};
use money2::{Exchange, ExchangeRates};
use sqlx::{Pool, Postgres, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{
		DepartmentColumns,
		EmployeeColumns,
		ExpenseColumns,
		JobColumns,
		JobDepartmentColumns,
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
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(connection: &Pool<Postgres>, match_condition: Self::Match) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: TimesheetColumns<&str> = TimesheetColumns::default();

		const DEPARTMENTS_AGGREGATED_ALIAS: &str = "D_A";
		const DEPARTMENTS_AGGREGATED_IDENT: &str = "departments_aggregated";
		const EXPENSES_AGGREGATED_IDENT: &str = "expenses_aggregated";

		const EMPLOYEE_DEPARTMENT_COLUMNS_UNIQUE: DepartmentColumns = DepartmentColumns::unique();
		const EMPLOYEE_COLUMNS_UNIQUE: EmployeeColumns<&str> = EmployeeColumns::unique();
		const JOB_COLUMNS_UNIQUE: JobColumns<&str> = JobColumns::unique();
		const ORGANIZATION_COLUMNS_UNIQUE: OrganizationColumns<&str> = OrganizationColumns::unique();

		let columns = COLUMNS.default_scope();
		let department_columns = DepartmentColumns::default().scope(DEPARTMENTS_AGGREGATED_ALIAS);
		let employee_columns = EmployeeColumns::default().default_scope();
		let employee_department_columns = DepartmentColumns::default().default_scope();
		let exchange_rates_fut = ExchangeRates::new().map_err(util::finance_err_to_sqlx);
		let expense_columns = ExpenseColumns::default().default_scope();
		let job_columns = JobColumns::default().default_scope();
		let job_department_columns = JobDepartmentColumns::default().default_scope();
		let location_columns = LocationColumns::default().default_scope();
		let match_location = match_condition.job.client.location.clone();
		let mut query = PgLocation::query_with_recursive(&match_location);
		let organization_columns = OrganizationColumns::default().default_scope();

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_more_columns(&employee_columns.r#as(EMPLOYEE_COLUMNS_UNIQUE))
			.push_more_columns(&employee_department_columns.r#as(EMPLOYEE_DEPARTMENT_COLUMNS_UNIQUE))
			.push(",array_agg((")
			.push_columns(&department_columns)
			.push("))")
			.push(sql::AS)
			.push(DEPARTMENTS_AGGREGATED_IDENT)
			.push(",array_agg((")
			.push_columns(&expense_columns)
			.push("))")
			.push(sql::AS)
			.push(EXPENSES_AGGREGATED_IDENT)
			.push_more_columns(&job_columns.r#as(JOB_COLUMNS_UNIQUE))
			.push_more_columns(&organization_columns.r#as(ORGANIZATION_COLUMNS_UNIQUE))
			.push_default_from::<TimesheetColumns>()
			.push_default_equijoin::<EmployeeColumns, _, _>(employee_columns.id, columns.employee_id)
			.push_default_equijoin::<DepartmentColumns, _, _>(
				employee_department_columns.id,
				employee_columns.department_id,
			)
			.push(sql::LEFT)
			.push_default_equijoin::<ExpenseColumns, _, _>(expense_columns.timesheet_id, columns.id)
			.push_default_equijoin::<JobColumns, _, _>(job_columns.id, columns.job_id)
			.push(sql::LEFT)
			.push_default_equijoin::<JobDepartmentColumns, _, _>(job_department_columns.job_id, job_columns.id)
			.push(sql::LEFT)
			.push_equijoin(
				DepartmentColumns::TABLE_NAME,
				DEPARTMENTS_AGGREGATED_ALIAS,
				department_columns.id,
				job_department_columns.department_id,
			)
			.push_default_equijoin::<OrganizationColumns, _, _>(organization_columns.id, job_columns.client_id)
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_location),
				LocationColumns::DEFAULT_ALIAS,
				location_columns.id,
				organization_columns.location_id,
			);

		let exchanged_condition =
			exchange_rates_fut.await.map(|rates| match_condition.exchange(Default::default(), &rates))?;

		PgSchema::write_where_clause(
			PgSchema::write_where_clause(
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
							DepartmentColumns::DEFAULT_ALIAS,
							&exchanged_condition.employee.department,
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
			),
			DEPARTMENTS_AGGREGATED_ALIAS,
			&exchanged_condition.job.departments,
			&mut query,
		);

		query
			.push(sql::GROUP_BY)
			.separated(',')
			.push(columns.id)
			.push(employee_columns.id)
			.push(employee_department_columns.id)
			.push(job_columns.id)
			.push(organization_columns.id);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move {
				Self::row_to_view(
					connection,
					COLUMNS,
					DEPARTMENTS_AGGREGATED_IDENT,
					EMPLOYEE_DEPARTMENT_COLUMNS_UNIQUE,
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

	use mockd::{address, company, job, name, words};
	use money2::{Exchange, ExchangeRates};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{
			DepartmentAdapter,
			EmployeeAdapter,
			JobAdapter,
			LocationAdapter,
			OrganizationAdapter,
			TimesheetAdapter,
		},
		Retrievable,
	};
	use winvoice_match::{Match, MatchDepartment, MatchSet, MatchTimesheet};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Currency,
		Invoice,
		InvoiceDate,
		Money,
	};

	use crate::schema::{util, PgDepartment, PgEmployee, PgJob, PgLocation, PgOrganization, PgTimesheet};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::state(), None).await.unwrap();
		let street = PgLocation::create(&connection, None, util::rand_street_name(), city.into()).await.unwrap();

		let (department, department2, location, location2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
		)
		.unwrap();

		let (organization, organization2) = futures::try_join!(
			PgOrganization::create(&connection, location.clone(), company::company()),
			PgOrganization::create(&connection, location2.clone(), company::company()),
		)
		.unwrap();

		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
			PgEmployee::create(&connection, department2.clone(), name::full(), job::title()),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let job = PgJob::create(
			&mut tx,
			organization.clone(),
			None,
			Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
			[department.clone()].into_iter().collect(),
			Duration::from_secs(900),
			Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let job2 = PgJob::create(
			&mut tx,
			organization2.clone(),
			Utc.with_ymd_and_hms(3000, 01, 13, 11, 30, 00).latest(),
			Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
			[department2.clone()].into_iter().collect(),
			Duration::from_secs(900),
			Invoice {
				date: InvoiceDate {
					issued: Utc.with_ymd_and_hms(3000, 01, 13, 11, 45, 00).unwrap(),
					paid: Utc.with_ymd_and_hms(3000, 01, 15, 14, 27, 00).latest(),
				}
				.into(),
				hourly_rate: Money::new(200_00, 2, Currency::Jpy),
			},
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let timesheet = PgTimesheet::create(&mut tx, employee, Vec::new(), job, Utc::now(), None, words::sentence(5))
			.await
			.unwrap();

		let timesheet2 = PgTimesheet::create(
			&mut tx,
			employee2,
			vec![(words::word(), Money::new(300_56, 2, Currency::Usd), words::sentence(5))],
			job2,
			Utc.with_ymd_and_hms(2022, 06, 08, 15, 27, 00).unwrap(),
			Utc.with_ymd_and_hms(2022, 06, 09, 07, 00, 00).latest(),
			words::sentence(5),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();
		// }}}

		let exchange_rates = ExchangeRates::new().await.unwrap();

		assert_eq!(
			PgTimesheet::retrieve(&connection, MatchTimesheet {
				expenses: !MatchSet::Contains(Default::default()),
				employee: (Match::from(timesheet.employee.id) | timesheet2.employee.id.into()).into(),
				work_notes: [].into_iter().collect(),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.as_slice(),
			&[timesheet.exchange(Default::default(), &exchange_rates)],
		);

		assert_eq!(
			PgTimesheet::retrieve(&connection, MatchTimesheet {
				job: MatchDepartment::from(department2.id).into(),
				employee: MatchDepartment::from(department2.name).into(),
				expenses: [].into_iter().collect(),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.as_slice(),
			&[timesheet2.exchange(Default::default(), &exchange_rates)],
		);
	}
}
