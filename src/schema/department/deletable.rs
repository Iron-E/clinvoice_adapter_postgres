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

	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
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
				(Match::from(department.id) | department2.id.into() | department3.id.into()).into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[department3],
		);
	}
}
