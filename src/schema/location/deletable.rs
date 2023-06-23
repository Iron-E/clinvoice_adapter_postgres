use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::LocationColumns, Deletable};
use winvoice_schema::Location;

use super::PgLocation;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgLocation
{
	type Db = Postgres;
	type Entity = Location;

	async fn delete<'connection, 'entity, Conn, Iter>(
		connection: Conn,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		fn mapper(l: &Location) -> PgUuid
		{
			PgUuid::from(l.id)
		}

		// TODO: use `for<'a> |e: &'a Location| e.id`
		PgSchema::delete::<_, _, LocationColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::address;
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::LocationAdapter, Deletable, Retrievable};
	use winvoice_match::Match;

	use crate::schema::{util, PgLocation};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::city(), None).await.unwrap();

		let (street, street2) = futures::try_join!(
			PgLocation::create(&connection, None, address::street(), city.clone().into()),
			PgLocation::create(&connection, None, address::street(), city.clone().into()),
		)
		.unwrap();

		assert!(PgLocation::delete(&connection, [&city].into_iter()).await.is_err());
		PgLocation::delete(&connection, [&street, &street2].into_iter()).await.unwrap();

		assert_eq!(
			PgLocation::retrieve(
				&connection,
				Match::Or([&city, &street, &street2].into_iter().map(|l| l.id.into()).collect())
					.into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[city]
		);
	}
}
