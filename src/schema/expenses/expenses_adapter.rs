use core::iter;

use money2::{Exchange, HistoricalExchangeRates, Money};
use sqlx::{Executor, Postgres, QueryBuilder, Result};
use winvoice_adapter::{fmt::QueryBuilderExt, schema::ExpensesAdapter};
use winvoice_schema::{
	chrono::{DateTime, Utc},
	Expense,
	Id,
};

use super::PgExpenses;
use crate::schema::util;

#[async_trait::async_trait]
impl ExpensesAdapter for PgExpenses
{
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn create<'connection, Conn>(
		connection: Conn,
		expenses: Vec<(String, Money, String)>,
		timesheet_id: Id,
		timesheet_time_begin: DateTime<Utc>,
	) -> Result<Vec<Expense>>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		if expenses.is_empty()
		{
			return Ok(Vec::new());
		}

		let rates = HistoricalExchangeRates::try_index(Some(timesheet_time_begin.into()))
			.await
			.map_err(util::finance_err_to_sqlx)?;

		let expenses_vec: Vec<_> = iter::from_fn(|| Id::new_v4().into())
			.take(expenses.len())
			.zip(expenses)
			.map(|(id, (category, cost, description))| {
				Expense { id, category, cost, description, timesheet_id }.exchange(Default::default(), &rates)
			})
			.collect();

		let mut query = QueryBuilder::new(
			"INSERT INTO expenses
				(id, timesheet_id, category, cost, description) ",
		);

		query.push_values(expenses_vec.iter(), |mut q, x| {
			q.push_bind(x.id)
				.push_bind(timesheet_id)
				.push_bind(&x.category)
				.push_bind(x.cost.amount.to_string())
				.push_bind(&x.description);
		});

		tracing::debug!("Generated SQL: {}", query.sql());
		query.prepare().execute(connection).await?;

		Ok(expenses_vec)
	}
}
