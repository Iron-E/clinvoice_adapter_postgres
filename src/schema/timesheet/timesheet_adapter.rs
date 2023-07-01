use money2::Money;
use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::schema::{ExpensesAdapter, TimesheetAdapter};
use winvoice_schema::{
	chrono::{DateTime, Utc},
	Employee,
	Id,
	Job,
	Timesheet,
};

use super::PgTimesheet;
use crate::{fmt::DateTimeExt, schema::PgExpenses};

#[async_trait::async_trait]
impl TimesheetAdapter for PgTimesheet
{
	async fn create(
		connection: &mut Transaction<Postgres>,
		employee: Employee,
		expenses: Vec<(String, Money, String)>,
		job: Job,
		time_begin: DateTime<Utc>,
		time_end: Option<DateTime<Utc>>,
		work_notes: String,
	) -> Result<Timesheet>
	{
		let id = Id::new_v4();
		sqlx::query!(
			"INSERT INTO timesheets
				(id, employee_id, job_id, time_begin, time_end, work_notes)
			VALUES
				($1, $2,          $3,     $4,         $5,       $6);",
			id,
			employee.id,
			job.id,
			time_begin.naive_utc(),
			time_end.map(|d| d.naive_utc()),
			work_notes,
		)
		.execute(&mut *connection)
		.await?;

		let expenses_db = PgExpenses::create(connection, expenses, id).await?;

		Ok(Timesheet { id, employee, expenses: expenses_db, job, time_begin, time_end, work_notes }.pg_sanitize())
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use mockd::{address, company, job, name, words};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::schema::{
		DepartmentAdapter,
		EmployeeAdapter,
		JobAdapter,
		LocationAdapter,
		OrganizationAdapter,
	};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Currency,
		Invoice,
		InvoiceDate,
		Money,
	};

	use super::{PgTimesheet, TimesheetAdapter};
	use crate::schema::{util, PgDepartment, PgEmployee, PgJob, PgLocation, PgOrganization};

	#[tokio::test]
	async fn retrieve()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::city(), None).await.unwrap();
		let street = PgLocation::create(&connection, None, util::rand_street_name(), city.into()).await.unwrap();

		let (location, location2) = futures::try_join!(
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
		)
		.unwrap();

		let (department, department2, organization, organization2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
			PgOrganization::create(&connection, location.clone(), company::company()),
			PgOrganization::create(&connection, location2.clone(), company::company()),
		)
		.unwrap();

		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
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

		macro_rules! select {
			($id:expr) => {
				sqlx::query!("SELECT * FROM timesheets WHERE id = $1", $id).fetch_one(&connection).await.unwrap()
			};
		}

		let timesheet_db = select!(timesheet.id);
		assert_eq!(timesheet_db.employee_id, timesheet.employee.id);
		assert_eq!(timesheet_db.id, timesheet.id);
		assert_eq!(timesheet_db.job_id, timesheet.job.id);
		assert_eq!(timesheet_db.time_begin.and_utc(), timesheet.time_begin);
		assert_eq!(timesheet_db.time_end.map(util::naive_date_to_utc), timesheet.time_end);
		assert_eq!(timesheet_db.work_notes, timesheet.work_notes);

		let timesheet2_db = select!(timesheet2.id);
		assert_eq!(timesheet2_db.employee_id, timesheet2.employee.id);
		assert_eq!(timesheet2_db.id, timesheet2.id);
		assert_eq!(timesheet2_db.job_id, timesheet2.job.id);
		assert_eq!(timesheet2_db.time_begin.and_utc(), timesheet2.time_begin);
		assert_eq!(timesheet2_db.time_end.map(util::naive_date_to_utc), timesheet2.time_end);
		assert_eq!(timesheet2_db.work_notes, timesheet2.work_notes);
	}
}
