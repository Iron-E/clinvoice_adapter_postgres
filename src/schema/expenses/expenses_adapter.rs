use core::iter;

use futures::TryFutureExt;
use money2::{Exchange, ExchangeRates, Money};
use sqlx::{Executor, Postgres, QueryBuilder, Result};
use winvoice_adapter::{
	fmt::QueryBuilderExt,
	schema::{columns::ExpenseColumns, ExpensesAdapter},
};
use winvoice_schema::{Expense, Id};

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

		let mut query = QueryBuilder::new(
			"INSERT INTO expenses
				(id, timesheet_id, category, cost, description) ",
		);

		let ids = iter::from_fn(|| Id::new_v4().into()).take(expenses.len()).collect::<Vec<_>>();
		query.push_values(
			ids.iter().zip(expenses.iter()),
			|mut q, (id, (category, cost, description))| {
				q.push_bind(id)
					.push_bind(timesheet_id)
					.push_bind(category)
					.push_bind(
						cost.exchange(Default::default(), &exchange_rates).amount.to_string(),
					)
					.push_bind(description);
			},
		);

		tracing::debug!("Generated SQL: {}", query.sql());
		query.prepare().execute(connection).await?;

		Ok(ids
			.into_iter()
			.zip(expenses.into_iter())
			.map(|(id, (category, cost, description))| Expense {
				id,
				category,
				cost,
				description,
				timesheet_id,
			})
			.collect())
	}
}
