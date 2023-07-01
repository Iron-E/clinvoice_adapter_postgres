use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::OrganizationColumns, Deletable};
use winvoice_schema::Organization;

use super::PgOrganization;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgOrganization
{
	type Db = Postgres;
	type Entity = Organization;

	async fn delete<'connection, 'entity, Conn, Iter>(connection: Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		fn mapper(o: &Organization) -> PgUuid
		{
			PgUuid::from(o.id)
		}

		// TODO: use `for<'a> |e: &'a Organization| e.id`
		PgSchema::delete::<_, _, OrganizationColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{address, company};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{LocationAdapter, OrganizationAdapter},
		Deletable,
		Retrievable,
	};
	use winvoice_match::Match;

	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let earth = PgLocation::create(&connection, None, address::country(), None).await.unwrap();

		let (organization, organization2, organization3) = futures::try_join!(
			PgOrganization::create(&connection, earth.clone(), company::company()),
			PgOrganization::create(&connection, earth.clone(), company::company()),
			PgOrganization::create(&connection, earth.clone(), company::company()),
		)
		.unwrap();

		// The `organization`s still depend on `earth`
		assert!(PgLocation::delete(&connection, [&earth].into_iter()).await.is_err());
		PgOrganization::delete(&connection, [&organization, &organization2].into_iter()).await.unwrap();

		assert_eq!(
			PgOrganization::retrieve(
				&connection,
				Match::Or([&organization, &organization2, &organization3,].into_iter().map(|o| o.id.into()).collect())
					.into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[organization3]
		);
	}
}
