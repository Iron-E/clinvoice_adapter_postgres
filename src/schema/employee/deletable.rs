use clinvoice_adapter::{schema::columns::EmployeeColumns, Deletable};
use clinvoice_schema::{Employee, Id};
use sqlx::{Executor, Postgres, Result};

use super::PgEmployee;
use crate::PgSchema;

#[async_trait::async_trait]
impl Deletable for PgEmployee
{
	type Db = Postgres;
	type Entity = Employee;

	async fn delete<'c, 'e, 'i, TConn, TIter>(connection: TConn, entities: TIter) -> Result<()>
	where
		'e: 'i,
		Self::Entity: 'e,
		TConn: Executor<'c, Database = Self::Db>,
		TIter: Iterator<Item = &'i Self::Entity> + Send,
	{
		fn mapper(e: &Employee) -> Id
		{
			e.id
		}

		// TODO: use `for<'a> |e: &'a Employee| e.id`
		PgSchema::delete::<_, _, EmployeeColumns<char>>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use clinvoice_adapter::{schema::EmployeeAdapter, Deletable, Retrievable};
	use clinvoice_match::{Match, MatchEmployee};
	use pretty_assertions::assert_eq;

	use crate::schema::{util, PgEmployee};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let (employee, employee2, employee3) = futures::try_join!(
			PgEmployee::create(
				&connection,
				"My Name".into(),
				"Employed".into(),
				"Janitor".into(),
			),
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

		PgEmployee::delete(&connection, [&employee, &employee2].into_iter())
			.await
			.unwrap();

		assert_eq!(
			PgEmployee::retrieve(&connection, &MatchEmployee {
				id: Match::Or(vec![
					employee.id.into(),
					employee2.id.into(),
					employee3.id.into()
				]),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[employee3],
		);
	}
}
