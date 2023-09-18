mod error;

use std::collections::BTreeSet;

use error::ContactInformationCheckViolation;
use futures::stream::TryStreamExt;
use sqlx::{Error, Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::LocationColumns, Deletable};
use winvoice_schema::Location;

use super::PgLocation;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgLocation
{
	type Db = Postgres;
	type Entity = Location;

	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
	{
		let ids: Vec<_> = entities.map(|l| l.id).collect();

		// HACK: postgres does not run CHECK constraints when deleting entries, so it may be possible to delete a
		//       location which is still being used for contact information.
		let skipped: BTreeSet<_> = sqlx::query!(
			"SELECT L.id
			FROM contact_information C
			JOIN locations L on (L.id = C.address_id)
			WHERE L.id = ANY($1);",
			ids.as_slice(),
		)
		.fetch(connection)
		.map_ok(|s| s.id)
		.try_collect()
		.await?;

		// TODO: use `for<'a> |e: &'a Location| e.id`
		PgSchema::delete::<_, _, LocationColumns>(
			connection,
			ids.into_iter().filter_map(|i| match skipped.contains(&i)
			{
				true => None,
				false => Some(PgUuid::from(i)),
			}),
		)
		.await?;

		match skipped.len()
		{
			0 => Ok(()),
			_ => Err(Error::Database(Box::new(ContactInformationCheckViolation::new(skipped)))),
		}
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
			PgLocation::retrieve(&connection, (Match::from(city.id) | street.id.into() | street2.id.into()).into(),)
				.await
				.unwrap()
				.as_slice(),
			&[city]
		);
	}
}
