mod deletable;
mod employee_adapter;
mod retrievable;
mod updatable;

use sqlx::{postgres::PgRow, Row};
use winvoice_adapter::schema::columns::{DepartmentColumns, EmployeeColumns};
use winvoice_schema::Employee;

use super::PgDepartment;

/// Implementor of the [`EmployeeAdapter`](winvoice_adapter::schema::EmployeeAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgEmployee;

impl PgEmployee
{
	/// Convert the `row` into a typed [`Employee`].
	pub fn row_to_view<EmployeeColumnT, DepartmentColumnT>(
		columns: EmployeeColumns<EmployeeColumnT>,
		department_columns: DepartmentColumns<DepartmentColumnT>,
		row: &PgRow,
	) -> Employee
	where
		DepartmentColumnT: AsRef<str>,
		EmployeeColumnT: AsRef<str>,
	{
		Employee {
			active: row.get(columns.active.as_ref()),
			department: PgDepartment::row_to_view(department_columns, row),
			id: row.get(columns.id.as_ref()),
			name: row.get(columns.name.as_ref()),
			title: row.get(columns.title.as_ref()),
		}
	}
}
