use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::ContactColumns, Updatable};
use winvoice_schema::Contact;

use super::PgContact;
use crate::PgSchema;

#[async_trait::async_trait]
impl Updatable for PgContact
{
	type Db = Postgres;
	type Entity = Contact;

	async fn update<'entity, Iter>(
		connection: &mut Transaction<Self::Db>,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		let mut peekable_entities = entities.peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		PgSchema::update(connection, ContactColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.kind.address().map(|a| a.id))
					.push_bind(e.kind.email())
					.push_bind(&e.label)
					.push_bind(e.kind.other())
					.push_bind(e.kind.phone());
			});
		})
		.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use mockd::{address, contact, words};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{ContactAdapter, LocationAdapter},
		Deletable,
		Retrievable,
		Updatable,
	};
	use winvoice_match::{MatchContact, MatchStr};
	use winvoice_schema::ContactKind;

	use crate::schema::{util, PgContact, PgLocation};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect();

		let (country, country2) = futures::try_join!(
			PgLocation::create(&connection, None, address::country(), None),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let (mut office, mut phone) = futures::try_join!(
			PgContact::create(&connection, ContactKind::Address(country), words::sentence(3),),
			PgContact::create(
				&connection,
				ContactKind::Phone(contact::phone()),
				words::sentence(3),
			),
		)
		.unwrap();

		office.kind = ContactKind::Address(country2);
		phone.kind = ContactKind::Email(contact::email());

		{
			let mut tx = connection.begin().await.unwrap();
			PgContact::update(&mut tx, [&office, &phone].into_iter()).await.unwrap();
			tx.commit().await.unwrap();
		}

		let db_contact_info: HashSet<_> = PgContact::retrieve(&connection, MatchContact {
			label: MatchStr::Or(
				[&office, &phone].into_iter().map(|c| c.label.clone().into()).collect(),
			),
			..Default::default()
		})
		.await
		.unwrap()
		.into_iter()
		.collect();

		assert_eq!([&office, &phone].into_iter().cloned().collect::<HashSet<_>>(), db_contact_info);

		// cleanup
		PgContact::delete(&connection, [&office, &phone].into_iter()).await.unwrap();
	}
}
