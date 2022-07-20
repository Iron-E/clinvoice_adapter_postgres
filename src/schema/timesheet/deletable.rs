use clinvoice_adapter::{schema::columns::TimesheetColumns, Deletable};
use clinvoice_schema::{Id, Timesheet};
use sqlx::{Executor, Postgres, Result};

use super::PgTimesheet;
use crate::PgSchema;

#[async_trait::async_trait]
impl Deletable for PgTimesheet
{
	type Db = Postgres;
	type Entity = Timesheet;

	async fn delete<'c, 'e, 'i, TConn, TIter>(connection: TConn, entities: TIter) -> Result<()>
	where
		'e: 'i,
		Self::Entity: 'e,
		TConn: Executor<'c, Database = Self::Db>,
		TIter: Iterator<Item = &'i Self::Entity> + Send,
	{
		fn mapper(t: &Timesheet) -> Id
		{
			t.id
		}

		// TODO: use `for<'a> |e: &'a Timesheet| e.id`
		PgSchema::delete::<_, _, TimesheetColumns<char>>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use clinvoice_adapter::{
		schema::{
			EmployeeAdapter,
			JobAdapter,
			LocationAdapter,
			OrganizationAdapter,
			TimesheetAdapter,
		},
		Deletable,
		Retrievable,
	};
	use clinvoice_finance::{Currency, ExchangeRates, Exchangeable, Money};
	use clinvoice_match::{Match, MatchExpense, MatchTimesheet};
	use clinvoice_schema::{
		chrono::{TimeZone, Utc},
		Invoice,
	};
	use pretty_assertions::assert_eq;

	use crate::schema::{
		util,
		PgEmployee,
		PgExpenses,
		PgJob,
		PgLocation,
		PgOrganization,
		PgTimesheet,
	};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let organization = PgOrganization::create(&connection, earth, "Some Organization".into())
			.await
			.unwrap();

		let employee = PgEmployee::create(
			&connection,
			"My Name".into(),
			"Employed".into(),
			"Janitor".into(),
		)
		.await
		.unwrap();

		let job = PgJob::create(
			&connection,
			organization.clone(),
			None,
			Utc.ymd(1990, 07, 12).and_hms(14, 10, 00),
			Duration::from_secs(900),
			Invoice {
				date: None,
				hourly_rate: Money::new(20_00, 2, Currency::Usd),
			},
			String::new(),
			"Do something".into(),
		)
		.await
		.unwrap();

		// {{{
		let mut transaction = connection.begin().await.unwrap();

		let timesheet = PgTimesheet::create(
			&mut transaction,
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
			&mut transaction,
			employee.clone(),
			vec![(
				"Flight".into(),
				Money::new(300_56, 2, Currency::Usd),
				"Trip to Hawaii for research".into(),
			)],
			job.clone(),
			Utc.ymd(2022, 06, 08).and_hms(15, 27, 00),
			Some(Utc.ymd(2022, 06, 09).and_hms(07, 00, 00)),
			"These are more work notes".into(),
		)
		.await
		.unwrap();

		let timesheet3 = PgTimesheet::create(
			&mut transaction,
			employee,
			vec![(
				"Food".into(),
				Money::new(10_17, 2, Currency::Usd),
				"Takeout".into(),
			)],
			job.clone(),
			Utc::now(),
			None,
			"Even more work notes".into(),
		)
		.await
		.unwrap();

		transaction.commit().await.unwrap();
		// }}}

		assert!(PgJob::delete(&connection, [job].iter()).await.is_err());
		PgTimesheet::delete(&connection, [&timesheet, &timesheet2].into_iter())
			.await
			.unwrap();

		let exchange_rates = ExchangeRates::new().await.unwrap();
		assert_eq!(
			PgTimesheet::retrieve(
				&connection,
				&MatchTimesheet::id(Match::Or(vec![
					timesheet.id.into(),
					timesheet2.id.into(),
					timesheet3.id.into(),
				]))
			)
			.await
			.unwrap()
			.into_iter()
			.as_slice(),
			&[timesheet3.exchange_ref(Default::default(), &exchange_rates)],
		);

		assert_eq!(
			PgExpenses::retrieve(&connection, &MatchExpense {
				timesheet_id: Match::Or(vec![
					timesheet.id.into(),
					timesheet2.id.into(),
					timesheet3.id.into(),
				]),
				..Default::default()
			})
			.await
			.unwrap(),
			timesheet3
				.expenses
				.exchange_ref(Default::default(), &exchange_rates),
		);
	}
}
