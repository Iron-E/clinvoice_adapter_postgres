use sqlx::{Postgres, QueryBuilder, Result, Transaction};
use winvoice_adapter::{fmt::QueryBuilderExt, schema::columns::TimesheetColumns, Updatable};
use winvoice_schema::{Expense, Timesheet};

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

	async fn update<'entity, Iter>(connection: &mut Transaction<Self::Db>, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		#![allow(clippy::items_after_statements)]

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

		// TODO: use `for<'a> |e: &'a Timesheet| &t.expenses` and `was_empty` var to avoid allocation
		let expenses = entities.clone().flat_map(mapper).collect::<Vec<_>>();
		fn mapper(t: &Timesheet) -> &[Expense]
		{
			&t.expenses
		}

		PgEmployee::update(connection, employees).await?;

		{
			let expenses = expenses.clone();
			PgExpenses::update(connection, expenses.iter().copied()).await?;
		}

		if !expenses.is_empty()
		{
			let mut builder = QueryBuilder::<Postgres>::new("DELETE FROM expenses WHERE id NOT IN (");
			{
				let mut sep = builder.separated(',');
				expenses.iter().for_each(|x| {
					sep.push_bind(x.id);
				});
			}

			builder.push(") AND timesheet_id IN (");
			{
				let mut sep = builder.separated(',');
				expenses.iter().for_each(|x| {
					sep.push_bind(x.timesheet_id);
				});
			}
			builder.push(')');

			tracing::debug!("Generated SQL: {}", builder.sql());
			builder.prepare().execute(&mut *connection).await?;
		}

		PgJob::update(connection, entities.map(|e| &e.job)).await
	}
}

#[cfg(test)]
mod tests
{
	use std::{collections::HashSet, time::Duration};

	use mockd::{address, company, job, name, words};
	use money2::{Currency, Money};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{
			DepartmentAdapter,
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
	use winvoice_schema::{chrono, Invoice, InvoiceDate};

	use crate::{
		fmt::DateTimeExt,
		schema::{util, PgDepartment, PgEmployee, PgExpenses, PgJob, PgLocation, PgOrganization, PgTimesheet},
	};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect();

		let (department, department2, location, location2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
			PgLocation::create(&connection, None, address::country(), None),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let organization = PgOrganization::create(&connection, location, company::company()).await.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let job = PgJob::create(
			&mut tx,
			organization,
			None,
			chrono::Utc::now(),
			[&department, &department2].into_iter().cloned().collect(),
			Duration::from_secs(900),
			Default::default(),
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();
		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(&connection, department, name::full(), job::title()),
			PgEmployee::create(&connection, department2.clone(), name::full(), job::title()),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();

		let mut timesheet = PgTimesheet::create(
			&mut tx,
			employee,
			vec![(words::word(), Money::new(500_00, 2, Currency::default()), words::sentence(5))],
			job,
			chrono::Utc::now(),
			None,
			words::sentence(5),
		)
		.await
		.unwrap();

		let new_expense = PgExpenses::create(
			&mut tx,
			vec![("category".into(), Money::default(), "description".into())],
			(timesheet.id, timesheet.time_begin),
		)
		.await
		.unwrap()
		.pop()
		.unwrap();

		timesheet.employee = employee2;
		timesheet.expenses.push(new_expense);
		timesheet.job.client.location = location2;
		timesheet.job.client.name = util::different_string(&timesheet.job.client.name);
		timesheet.job.date_close = chrono::Utc::now().into();
		timesheet.job.departments.remove(&department2);
		timesheet.job.increment = Duration::from_secs(300);
		timesheet.job.invoice = Invoice {
			date: InvoiceDate {
				issued: chrono::Utc::now(),
				paid: Some(chrono::Utc::now() + chrono::Duration::seconds(300)),
			}
			.into(),
			hourly_rate: Money::new(200_00, 2, Default::default()),
		};
		timesheet.job.notes = util::different_string(&timesheet.job.notes);
		timesheet.job.objectives = util::different_string(&timesheet.job.notes);
		timesheet.time_end = chrono::Utc::now().into();
		timesheet.work_notes = "Updated work notes".into();

		PgTimesheet::update(&mut tx, [&timesheet].into_iter()).await.unwrap();
		tx.commit().await.unwrap();

		let db_timesheet = PgTimesheet::retrieve(&connection, timesheet.id.into()).await.unwrap().pop().unwrap();

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
