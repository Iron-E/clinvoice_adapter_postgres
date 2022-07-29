use clinvoice_adapter::{schema::columns::OrganizationColumns, Deletable};
use clinvoice_schema::{Id, Organization};
use sqlx::{Executor, Postgres, Result};

use super::PgOrganization;
use crate::PgSchema;

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
		const fn mapper(o: &Organization) -> Id
		{
			o.id
		}

		// TODO: use `for<'a> |e: &'a Organization| e.id`
		PgSchema::delete::<_, _, OrganizationColumns<char>>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use clinvoice_adapter::{
		schema::{LocationAdapter, OrganizationAdapter},
		Deletable,
		Retrievable,
	};
	use clinvoice_match::Match;
	use pretty_assertions::assert_eq;

	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let (organization, organization2, organization3) = futures::try_join!(
			PgOrganization::create(&connection, earth.clone(), "Some Organization".into()),
			PgOrganization::create(&connection, earth.clone(), "Some Other Organization".into()),
			PgOrganization::create(
				&connection,
				earth.clone(),
				"Another Other Organization".into(),
			),
		)
		.unwrap();

		// The `organization`s still depend on `earth`
		assert!(PgLocation::delete(&connection, [&earth].into_iter())
			.await
			.is_err());
		PgOrganization::delete(&connection, [&organization, &organization2].into_iter())
			.await
			.unwrap();

		assert_eq!(
			PgOrganization::retrieve(
				&connection,
				&Match::Or(vec![
					organization.id.into(),
					organization2.id.into(),
					organization3.id.into()
				])
				.into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[organization3]
		);
	}
}
