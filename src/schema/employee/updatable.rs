use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::EmployeeColumns, Updatable};
use winvoice_schema::Employee;

use super::PgEmployee;
use crate::{schema::PgDepartment, PgSchema};

#[async_trait::async_trait]
impl Updatable for PgEmployee
{
	type Db = Postgres;
	type Entity = Employee;

	async fn update<'entity, Iter>(connection: &mut Transaction<Self::Db>, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		let mut peekable_entities = entities.clone().peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		PgSchema::update(connection, EmployeeColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.active).push_bind(e.department.id).push_bind(e.id).push_bind(&e.name).push_bind(&e.title);
			});
		})
		.await?;

		PgDepartment::update(connection, entities.map(|e| &e.department)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{job, name};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{DepartmentAdapter, EmployeeAdapter},
		Retrievable,
		Updatable,
	};

	use crate::schema::{util, PgDepartment, PgEmployee};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect();

		let department = PgDepartment::create(&connection, util::rand_department_name()).await.unwrap();

		let mut employee = PgEmployee::create(&connection, department, name::full(), job::title()).await.unwrap();

		employee.active = !employee.active;
		employee.department.name = util::different_string(&employee.department.name);
		employee.name = util::different_string(&employee.name);
		employee.title = util::different_string(&employee.title);

		{
			let mut tx = connection.begin().await.unwrap();
			PgEmployee::update(&mut tx, [&employee].into_iter()).await.unwrap();
			tx.commit().await.unwrap();
		}

		let db_employee = PgEmployee::retrieve(&connection, employee.id.into()).await.unwrap().pop().unwrap();

		assert_eq!(employee, db_employee);
	}
}
