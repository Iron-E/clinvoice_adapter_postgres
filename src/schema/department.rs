mod deletable;
mod department_adapter;
mod retrievable;
mod updatable;

use sqlx::{postgres::PgRow, Row};
use winvoice_adapter::schema::columns::DepartmentColumns;
use winvoice_schema::Department;

/// Implementor of the [`DepartmentAdapter`](winvoice_adapter::schema::DepartmentAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgDepartment;

impl PgDepartment
{
	/// Convert the `row` into a typed [`Department`].
	pub fn row_to_view<T>(columns: DepartmentColumns<T>, row: &PgRow) -> Department
	where
		T: AsRef<str>,
	{
		Department { id: row.get(columns.id.as_ref()), name: row.get(columns.name.as_ref()) }
	}
}
