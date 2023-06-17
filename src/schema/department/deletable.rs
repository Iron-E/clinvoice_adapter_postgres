use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::DepartmentColumns, Deletable};
use winvoice_schema::Department;

use super::PgDepartment;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgDepartment
{
	type Db = Postgres;
	type Entity = Department;

	async fn delete<'connection, 'entity, Conn, Iter>(
		connection: Conn,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		fn mapper(e: &Department) -> PgUuid
		{
			PgUuid::from(e.id)
		}

		// TODO: use `for<'a> |e: &'a Department| e.id`
		PgSchema::delete::<_, _, DepartmentColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::job;
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::DepartmentAdapter, Deletable, Retrievable};
	use winvoice_match::Match;

	use crate::schema::{util, PgDepartment};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let (department, department2, department3) = futures::try_join!(
			PgDepartment::create(&connection, job::level()),
			PgDepartment::create(&connection, job::level()),
			PgDepartment::create(&connection, job::level()),
		)
		.unwrap();

		PgDepartment::delete(&connection, [&department, &department2].into_iter()).await.unwrap();

		assert_eq!(
			PgDepartment::retrieve(
				&connection,
				Match::Or(vec![department.id.into(), department2.id.into(), department3.id.into()])
					.into()
			)
			.await
			.unwrap()
			.as_slice(),
			&[department3],
		);
	}
}
