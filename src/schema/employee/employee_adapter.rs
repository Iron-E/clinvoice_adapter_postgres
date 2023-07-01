use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::EmployeeAdapter;
use winvoice_schema::{Department, Employee, Id};

use super::PgEmployee;

#[async_trait::async_trait]
impl EmployeeAdapter for PgEmployee
{
	async fn create<'connection, Conn>(
		connection: Conn,
		department: Department,
		name: String,
		title: String,
	) -> Result<Employee>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let active = true;
		let id = Id::new_v4();
		sqlx::query!(
			"INSERT INTO employees (id, active, department_id, name, title) VALUES ($1, $2, $3, $4, $5);",
			id,
			active,
			department.id,
			name,
			title,
		)
		.execute(connection)
		.await?;

		Ok(Employee { active, id, department, name, title })
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{job, name};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::schema::DepartmentAdapter;

	use super::{EmployeeAdapter, PgEmployee};
	use crate::schema::{util, PgDepartment};

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect();

		let department = PgDepartment::create(&connection, util::rand_department_name()).await.unwrap();

		let employee = PgEmployee::create(&connection, department, name::full(), job::title()).await.unwrap();

		let row =
			sqlx::query!("SELECT * FROM employees WHERE id = $1;", employee.id).fetch_one(&connection).await.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(employee.active, row.active);
		assert_eq!(employee.department.id, row.department_id);
		assert_eq!(employee.id, row.id);
		assert_eq!(employee.name, row.name);
		assert_eq!(employee.title, row.title);
	}
}
