use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::TimesheetColumns, Deletable};
use winvoice_schema::Timesheet;

use super::PgTimesheet;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgTimesheet
{
	type Db = Postgres;
	type Entity = Timesheet;

	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
	{
		fn mapper(t: &Timesheet) -> PgUuid
		{
			PgUuid::from(t.id)
		}

		// TODO: use `for<'a> |e: &'a Timesheet| e.id`
		PgSchema::delete::<_, _, TimesheetColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use mockd::{address, company, job, name, words};
	use money2::{Currency, Exchange, ExchangeRates, Money};
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
		Deletable,
		Retrievable,
	};
	use winvoice_match::{Match, MatchExpense};
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Invoice,
	};

	use crate::schema::{util, PgDepartment, PgEmployee, PgExpenses, PgJob, PgLocation, PgOrganization, PgTimesheet};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let location = PgLocation::create(&connection, None, address::country(), None).await.unwrap();

		let (department, organization) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgOrganization::create(&connection, location, company::company()),
		)
		.unwrap();

		let employee = PgEmployee::create(&connection, department.clone(), name::full(), job::title()).await.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let job = PgJob::create(
			&mut tx,
			organization.clone(),
			None,
			Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
			[department].into_iter().collect(),
			Duration::from_secs(900),
			Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let timesheet = PgTimesheet::create(
			&mut tx,
			employee.clone(),
			Vec::new(),
			job.clone(),
			Utc::now(),
			None,
			"These are my work notes".into(),
		)
		.await
		.unwrap();

		let timesheet2 = PgTimesheet::create(
			&mut tx,
			employee.clone(),
			vec![("Flight".into(), Money::new(300_56, 2, Currency::Usd), "Trip to Hawaii for research".into())],
			job.clone(),
			Utc.with_ymd_and_hms(2022, 06, 08, 15, 27, 00).unwrap(),
			Utc.with_ymd_and_hms(2022, 06, 09, 07, 00, 00).latest(),
			"These are more work notes".into(),
		)
		.await
		.unwrap();

		let timesheet3 = PgTimesheet::create(
			&mut tx,
			employee,
			vec![("Food".into(), Money::new(10_17, 2, Currency::Usd), "Takeout".into())],
			job.clone(),
			Utc::now(),
			None,
			"Even more work notes".into(),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();
		// }}}

		PgTimesheet::delete(&connection, [&timesheet, &timesheet2].into_iter()).await.unwrap();

		let exchange_rates = ExchangeRates::new().await.unwrap();
		assert_eq!(
			PgTimesheet::retrieve(
				&connection,
				(Match::from(timesheet.id) | timesheet2.id.into() | timesheet3.id.into()).into(),
			)
			.await
			.unwrap()
			.into_iter()
			.as_slice(),
			&[timesheet3.clone().exchange(Default::default(), &exchange_rates)],
		);

		assert_eq!(
			PgExpenses::retrieve(&connection, MatchExpense {
				timesheet_id: Match::from(timesheet.id) | timesheet2.id.into() | timesheet3.id.into(),
				..Default::default()
			})
			.await
			.unwrap(),
			timesheet3.expenses.exchange(Default::default(), &exchange_rates),
		);
	}
}
