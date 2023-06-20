use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::OrganizationColumns, Updatable};
use winvoice_schema::Organization;

use super::PgOrganization;
use crate::{schema::PgLocation, PgSchema};

#[async_trait::async_trait]
impl Updatable for PgOrganization
{
	type Db = Postgres;
	type Entity = Organization;

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

		PgSchema::update(connection, OrganizationColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.id).push_bind(e.location.id).push_bind(&e.name);
			});
		})
		.await?;

		PgLocation::update(connection, entities.map(|e| &e.location)).await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{address, company};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{LocationAdapter, OrganizationAdapter},
		Retrievable,
		Updatable,
	};

	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect().await;

		let (earth, mars) = futures::try_join!(
			PgLocation::create(&connection, None, address::street(), None),
			PgLocation::create(&connection, None, address::street(), None),
		)
		.unwrap();

		let mut organization =
			PgOrganization::create(&connection, earth, company::company()).await.unwrap();

		organization.location = mars;
		organization.name = util::different_string(&organization.name);

		{
			let mut tx = connection.begin().await.unwrap();
			PgOrganization::update(&mut tx, [&organization].into_iter()).await.unwrap();
			tx.commit().await.unwrap();
		}

		assert_eq!(
			PgOrganization::retrieve(&connection, organization.id.into()).await.unwrap().as_slice(),
			&[organization]
		);
	}
}
