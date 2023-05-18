use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt},
	schema::{columns::ExpenseColumns, ExpensesAdapter},
};
use winvoice_schema::{Expense, Id};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use money2::{Exchange, ExchangeRates, Money};
use sqlx::{Executor, Postgres, QueryBuilder, Result, Row};

use super::PgExpenses;
use crate::schema::util;

#[async_trait::async_trait]
impl ExpensesAdapter for PgExpenses
{
	async fn create<'connection, Conn>(
		connection: Conn,
		expenses: Vec<(String, Money, String)>,
		timesheet_id: Id,
	) -> Result<Vec<Expense>>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		const COLUMNS: ExpenseColumns<&'static str> = ExpenseColumns::default();

		if expenses.is_empty()
		{
			return Ok(Vec::new());
		}

		let exchange_rates = ExchangeRates::new().map_err(util::finance_err_to_sqlx).await?;

		QueryBuilder::new(
			"INSERT INTO expenses
				(timesheet_id, category, cost, description) ",
		)
		.push_values(expenses.iter(), |mut q, (category, cost, description)| {
			q.push_bind(timesheet_id)
				.push_bind(category)
				.push_bind(cost.exchange(Default::default(), &exchange_rates).amount.to_string())
				.push_bind(description);
		})
		.push(sql::RETURNING)
		.push(COLUMNS.id)
		.prepare()
		.fetch(connection)
		.zip(stream::iter(expenses.iter()))
		.map(|(result, (category, cost, description))| {
			result.map(|row| Expense {
				category: category.clone(),
				cost: *cost,
				description: description.clone(),
				id: row.get(COLUMNS.id),
				timesheet_id,
			})
		})
		.try_collect::<Vec<_>>()
		.await
	}
}
