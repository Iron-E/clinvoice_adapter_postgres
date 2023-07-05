use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::EmployeeColumns, Deletable};
use winvoice_schema::Employee;

use super::PgEmployee;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgEmployee
{
	type Db = Postgres;
	type Entity = Employee;

	async fn delete<'connection, 'entity, Conn, Iter>(connection: Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		fn mapper(e: &Employee) -> PgUuid
		{
			PgUuid::from(e.id)
		}

		// TODO: use `for<'a> |e: &'a Employee| e.id`
		PgSchema::delete::<_, _, EmployeeColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{job, name};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{DepartmentAdapter, EmployeeAdapter},
		Deletable,
		Retrievable,
	};
	use winvoice_match::Match;

	use crate::schema::{util, PgDepartment, PgEmployee};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let (department, department2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
		)
		.unwrap();

		let (employee, employee2, employee3) = futures::try_join!(
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
			PgEmployee::create(&connection, department, name::full(), job::title()),
			PgEmployee::create(&connection, department2, name::full(), job::title()),
		)
		.unwrap();

		PgEmployee::delete(&connection, [&employee, &employee2].into_iter()).await.unwrap();

		assert_eq!(
			PgEmployee::retrieve(
				&connection,
				(Match::from(employee.id) | employee2.id.into() | employee3.id.into()).into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[employee3],
		);
	}
}
