mod deletable;
mod employee_adapter;
mod retrievable;
mod updatable;

use winvoice_adapter::schema::columns::EmployeeColumns;
use winvoice_schema::Employee;
use sqlx::{postgres::PgRow, Row};

/// Implementor of the [`EmployeeAdapter`](winvoice_adapter::schema::EmployeeAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgEmployee;

impl PgEmployee
{
	pub(super) fn row_to_view<T>(columns: EmployeeColumns<T>, row: &PgRow) -> Employee
	where
		T: AsRef<str>,
	{
		Employee {
			id: row.get(columns.id.as_ref()),
			name: row.get(columns.name.as_ref()),
			status: row.get(columns.status.as_ref()),
			title: row.get(columns.title.as_ref()),
		}
	}
}
