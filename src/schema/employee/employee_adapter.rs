use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::EmployeeAdapter;
use winvoice_schema::{Employee, Id};

use super::PgEmployee;

#[async_trait::async_trait]
impl EmployeeAdapter for PgEmployee
{
	async fn create<'connection, Conn>(
		connection: Conn,
		name: String,
		status: String,
		title: String,
	) -> Result<Employee>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let id = Id::new_v4();
		sqlx::query!(
			"INSERT INTO employees (id, name, status, title) VALUES ($1, $2, $3, $4);",
			id,
			name,
			status,
			title,
		)
		.execute(connection)
		.await?;

		Ok(Employee { id, name, status, title })
	}
}

#[cfg(test)]
mod tests
{
	use pretty_assertions::assert_eq;

	use super::{EmployeeAdapter, PgEmployee};
	use crate::schema::util;

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect().await;

		let employee =
			PgEmployee::create(&connection, "My Name".into(), "Employed".into(), "Janitor".into())
				.await
				.unwrap();

		let row = sqlx::query!("SELECT * FROM employees WHERE id = $1;", employee.id)
			.fetch_one(&connection)
			.await
			.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(employee.id, row.id);
		assert_eq!(employee.name, row.name);
		assert_eq!(employee.status, row.status);
		assert_eq!(employee.title, row.title);
	}
}
