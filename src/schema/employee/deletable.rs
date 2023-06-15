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

	async fn delete<'connection, 'entity, Conn, Iter>(
		connection: Conn,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		const fn mapper(e: &Employee) -> PgUuid
		{
			PgUuid(e.id)
		}

		// TODO: use `for<'a> |e: &'a Employee| e.id`
		PgSchema::delete::<_, _, EmployeeColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::EmployeeAdapter, Deletable, Retrievable};
	use winvoice_match::Match;

	use crate::schema::{util, PgEmployee};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let (employee, employee2, employee3) = futures::try_join!(
			PgEmployee::create(&connection, "My Name".into(), "Employed".into(), "Janitor".into(),),
			PgEmployee::create(
				&connection,
				"Another Gúy".into(),
				"Management".into(),
				"Assistant to Regional Manager".into(),
			),
			PgEmployee::create(
				&connection,
				"Another Another Gúy".into(),
				"Management".into(),
				"Assistant to the Assistant to the Regional Manager".into(),
			),
		)
		.unwrap();

		PgEmployee::delete(&connection, [&employee, &employee2].into_iter()).await.unwrap();

		assert_eq!(
			PgEmployee::retrieve(
				&connection,
				Match::Or(vec![employee.id.into(), employee2.id.into(), employee3.id.into()])
					.into()
			)
			.await
			.unwrap()
			.as_slice(),
			&[employee3],
		);
	}
}
