use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::DepartmentColumns, Updatable};
use winvoice_schema::Department;

use super::PgDepartment;
use crate::PgSchema;

#[async_trait::async_trait]
impl Updatable for PgDepartment
{
	type Db = Postgres;
	type Entity = Department;

	async fn update<'entity, Iter>(
		connection: &mut Transaction<Self::Db>,
		entities: Iter,
	) -> Result<()>
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

		PgSchema::update(connection, DepartmentColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.id).push_bind(&e.name);
			});
		})
		.await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::job;
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::DepartmentAdapter, Retrievable, Updatable};

	use crate::schema::{util, PgDepartment};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect().await;

		let mut department = PgDepartment::create(&connection, job::level()).await.unwrap();

		department.name = format!("Not {}", department.name);

		{
			let mut transaction = connection.begin().await.unwrap();
			// PANICS: not implemented
			PgDepartment::update(&mut transaction, [&department].into_iter()).await.unwrap();
			transaction.commit().await.unwrap();
		}

		let db_department =
			PgDepartment::retrieve(&connection, department.id.into()).await.unwrap().pop().unwrap();

		assert_eq!(department, db_department);
	}
}
