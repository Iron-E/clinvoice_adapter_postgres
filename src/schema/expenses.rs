mod deletable;
mod expenses_adapter;
mod retrievable;
mod updatable;

use money2::{Decimal, Money};
use sqlx::{postgres::PgRow, Result, Row};
use winvoice_adapter::schema::columns::ExpenseColumns;
use winvoice_schema::Expense;

use super::util;

/// Implementor of the [`ExpensesAdapter`](winvoice_adapter::schema::ExpensesAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgExpenses;

impl PgExpenses
{
	/// Convert the `row` into a typed [`Expense`].
	pub fn row_to_view(columns: ExpenseColumns<&str>, row: &PgRow) -> Result<Expense>
	{
		Ok(Expense {
			id: row.try_get(columns.id)?,
			timesheet_id: row.try_get(columns.timesheet_id)?,
			category: row.try_get(columns.category)?,
			cost: Money {
				amount: row
					.try_get::<String, _>(columns.cost)
					.and_then(|cost| cost.parse::<Decimal>().map_err(|e| util::finance_err_to_sqlx(e.into())))?,
				..Default::default()
			},
			description: row.try_get(columns.description)?,
		})
	}
}
