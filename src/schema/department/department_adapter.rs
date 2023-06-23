use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::DepartmentAdapter;
use winvoice_schema::{Department, Id};

use super::PgDepartment;

#[async_trait::async_trait]
impl DepartmentAdapter for PgDepartment
{
	async fn create<'connection, Conn>(connection: Conn, name: String) -> Result<Department>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let id = Id::new_v4();
		sqlx::query!("INSERT INTO departments (id, name) VALUES ($1, $2);", id, name,)
			.execute(connection)
			.await?;

		Ok(Department { id, name })
	}
}

#[cfg(test)]
mod tests
{
	use pretty_assertions::assert_eq;

	use super::{DepartmentAdapter, PgDepartment};
	use crate::schema::util;

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect();

		let department =
			PgDepartment::create(&connection, util::rand_department_name()).await.unwrap();

		let row = sqlx::query!("SELECT * FROM departments WHERE id = $1;", department.id)
			.fetch_one(&connection)
			.await
			.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(department.id, row.id);
		assert_eq!(department.name, row.name);
	}
}
