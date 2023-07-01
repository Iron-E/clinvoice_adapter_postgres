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

	async fn delete<'connection, 'entity, Conn, Iter>(connection: Conn, entities: Iter) -> Result<()>
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
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::DepartmentAdapter, Deletable, Retrievable};
	use winvoice_match::Match;

	use crate::schema::{util, PgDepartment};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let (department, department2, department3) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
		)
		.unwrap();

		PgDepartment::delete(&connection, [&department, &department2].into_iter()).await.unwrap();

		assert_eq!(
			PgDepartment::retrieve(
				&connection,
				Match::Or([&department, &department2, &department3].into_iter().map(|d| d.id.into()).collect()).into()
			)
			.await
			.unwrap()
			.as_slice(),
			&[department3],
		);
	}
}
