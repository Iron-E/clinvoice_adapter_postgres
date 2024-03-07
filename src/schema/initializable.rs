use sqlx::{Acquire, Postgres, Result};
use winvoice_adapter::Initializable;

use super::PgSchema;

#[async_trait::async_trait]
impl Initializable for PgSchema {
	type Db = Postgres;

	async fn init<'connection, Conn>(connection: Conn) -> Result<()>
	where
		Conn: Acquire<'connection, Database = Self::Db> + Send,
	{
		let mut tx = connection.begin().await?;

		sqlx::query_file!("src/schema/initializable/00-locations.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/01-organizations.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/02-departments.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/03-employees.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/04-contact-information.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/05-money.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/06-jobs.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/07-job_departments.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/08-timesheets.sql").execute(&mut tx).await?;
		sqlx::query_file!("src/schema/initializable/09-expenses.sql").execute(&mut tx).await?;

		tx.commit().await
	}
}
