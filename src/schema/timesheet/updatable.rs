use clinvoice_adapter::{schema::columns::TimesheetColumns, Updatable};
use clinvoice_schema::{Expense, Timesheet};
use sqlx::{Postgres, Result, Transaction};

use super::PgTimesheet;
use crate::{
	schema::{PgEmployee, PgExpenses, PgJob},
	PgSchema,
};

#[async_trait::async_trait]
impl Updatable for PgTimesheet
{
	type Db = Postgres;
	type Entity = Timesheet;

	async fn update<'e, 'i, TIter>(
		connection: &mut Transaction<Self::Db>,
		entities: TIter,
	) -> Result<()>
	where
		'e: 'i,
		Self::Entity: 'e,
		TIter: Clone + Iterator<Item = &'i Self::Entity> + Send,
	{
		let mut peekable_entities = entities.clone().peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		PgSchema::update(connection, TimesheetColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.employee.id)
					.push_bind(e.id)
					.push_bind(e.job.id)
					.push_bind(e.time_begin)
					.push_bind(e.time_end)
					.push_bind(&e.work_notes);
			});
		})
		.await?;

		let employees = entities.clone().map(|e| &e.employee);

		// TODO: use `for<'a> |e: &'a Timesheet| &t.expenses`
		let expenses = entities.clone().flat_map(mapper);
		fn mapper(t: &Timesheet) -> &[Expense]
		{
			&t.expenses
		}

		PgEmployee::update(connection, employees).await?;
		PgExpenses::update(connection, expenses).await?;
		PgJob::update(connection, entities.map(|e| &e.job)).await
	}
}

#[cfg(test)]
mod tests
{
	use std::{collections::HashSet, time::Duration};

	use clinvoice_adapter::{
		schema::{
			EmployeeAdapter,
			ExpensesAdapter,
			JobAdapter,
			LocationAdapter,
			OrganizationAdapter,
			TimesheetAdapter,
		},
		Retrievable,
		Updatable,
	};
	use clinvoice_finance::{Currency, Money};
	use clinvoice_match::MatchTimesheet;
	use clinvoice_schema::{chrono, Invoice, InvoiceDate};
	use futures::TryFutureExt;
	use pretty_assertions::assert_eq;

	use crate::{
		fmt::DateTimeExt,
		schema::{util, PgEmployee, PgExpenses, PgJob, PgLocation, PgOrganization, PgTimesheet},
	};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect().await;

		let (earth, mars) = futures::try_join!(
			PgLocation::create(&connection, "Earth".into(), None),
			PgLocation::create(&connection, "Mars".into(), None),
		)
		.unwrap();

		let job = PgOrganization::create(&connection, earth, "Some Organization".into())
			.and_then(|organization| {
				PgJob::create(
					&connection,
					organization,
					None,
					chrono::Utc::now(),
					Duration::from_secs(900),
					Default::default(),
					Default::default(),
					Default::default(),
				)
			})
			.await
			.unwrap();

		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(
				&connection,
				"My Name".into(),
				"Employed".into(),
				"Janitor".into(),
			),
			PgEmployee::create(
				&connection,
				"Not My Name".into(),
				"Not Employed".into(),
				"Not Janitor".into(),
			),
		)
		.unwrap();

		// {{{
		let mut transaction = connection.begin().await.unwrap();

		let mut timesheet = PgTimesheet::create(
			&mut transaction,
			employee,
			vec![(
				"Travel".into(),
				Money::new(500_00, 2, Currency::default()),
				"Flight".into(),
			)],
			job,
			chrono::Utc::now(),
			None,
		)
		.await
		.unwrap();

		transaction.commit().await.unwrap();
		// }}}

		let new_expense = PgExpenses::create(
			&connection,
			vec![("category".into(), Money::default(), "description".into())],
			timesheet.id,
		)
		.await
		.unwrap()
		.pop()
		.unwrap();

		timesheet.employee = employee2;
		timesheet.job.client.location = mars;
		timesheet.job.client.name = format!("Not {}", timesheet.job.client.name);
		timesheet.job.date_close = Some(chrono::Utc::now());
		timesheet.job.increment = Duration::from_secs(300);
		timesheet.job.invoice = Invoice {
			date: Some(InvoiceDate {
				issued: chrono::Utc::now(),
				paid: Some(chrono::Utc::now() + chrono::Duration::seconds(300)),
			}),
			hourly_rate: Money::new(200_00, 2, Default::default()),
		};
		timesheet.job.notes = format!("Finished {}", timesheet.job.notes);
		timesheet.job.objectives = format!("Test {}", timesheet.job.notes);
		timesheet.time_end = Some(chrono::Utc::now());

		timesheet.expenses.push(new_expense);

		{
			let mut transaction = connection.begin().await.unwrap();
			PgTimesheet::update(&mut transaction, [&timesheet].into_iter())
				.await
				.unwrap();
			transaction.commit().await.unwrap();
		}

		let db_timesheet = PgTimesheet::retrieve(&connection, &MatchTimesheet {
			id: timesheet.id.into(),
			..Default::default()
		})
		.await
		.unwrap()
		.pop()
		.unwrap();

		assert_eq!(timesheet.id, db_timesheet.id);
		assert_eq!(timesheet.employee, db_timesheet.employee);
		assert_eq!(
			timesheet.expenses.into_iter().collect::<HashSet<_>>(),
			db_timesheet.expenses.into_iter().collect::<HashSet<_>>()
		);
		assert_eq!(timesheet.job.pg_sanitize(), db_timesheet.job);
		assert_eq!(timesheet.time_begin.pg_sanitize(), db_timesheet.time_begin);
		assert_eq!(timesheet.time_end.pg_sanitize(), db_timesheet.time_end);
		assert_eq!(timesheet.work_notes, db_timesheet.work_notes);
	}
}
