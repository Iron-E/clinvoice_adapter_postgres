use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::ExpenseColumns, Deletable};
use winvoice_schema::Expense;

use super::PgExpenses;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgExpenses
{
	type Db = Postgres;
	type Entity = Expense;

	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
	{
		fn mapper(x: &Expense) -> PgUuid
		{
			PgUuid::from(x.id)
		}

		// TODO: use `for<'a> |e: &'a Expense| e.id`
		PgSchema::delete::<_, _, ExpenseColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use mockd::{address, company, job, name, words};
	use money2::{Currency, Exchange, HistoricalExchangeRates, Money};
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
	use winvoice_match::MatchExpense;
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Invoice,
	};

	use crate::schema::{util, PgDepartment, PgEmployee, PgExpenses, PgJob, PgLocation, PgOrganization, PgTimesheet};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let (department, location) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let (employee, organization) = futures::try_join!(
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
			PgOrganization::create(&connection, location, company::company()),
		)
		.unwrap();

		let timesheet = {
			let mut tx = connection.begin().await.unwrap();

			let job_ = PgJob::create(
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
				vec![
					("Flight".into(), Money::new(300_56, 2, Currency::Jpy), "Trip to Hawaii for research".into()),
					("Food".into(), Money::new(10_17, 2, Currency::Usd), "Takeout".into()),
					("Taxi".into(), Money::new(563_30, 2, Currency::Nok), "Took a taxi cab".into()),
				],
				job_,
				Utc.with_ymd_and_hms(2022, 06, 08, 15, 27, 00).unwrap(),
				Utc.with_ymd_and_hms(2022, 06, 09, 07, 00, 00).latest(),
				"These are my work notes".into(),
			)
			.await
			.unwrap();

			tx.commit().await.unwrap();
			timesheet
		};

		PgExpenses::delete(&connection, timesheet.expenses.iter().take(2)).await.unwrap();

		let exchange_rates = HistoricalExchangeRates::index(None).await;

		assert_eq!(
			PgExpenses::retrieve(&connection, MatchExpense { timesheet_id: timesheet.id.into(), ..Default::default() })
				.await
				.unwrap()
				.into_iter()
				.filter(|x| x.timesheet_id == timesheet.id)
				.collect::<Vec<_>>(),
			timesheet
				.expenses
				.into_iter()
				.skip(2)
				.map(|e| e.exchange(Default::default(), &exchange_rates))
				.collect::<Vec<_>>(),
		);
	}
}
