use core::fmt::Display;

use sqlx::{query_builder::Separated, Executor, Postgres, QueryBuilder, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::ContactColumns,
	Deletable,
};
use winvoice_schema::Contact;

use super::PgContact;

#[async_trait::async_trait]
impl Deletable for PgContact
{
	type Db = Postgres;
	type Entity = Contact;

	#[tracing::instrument(level = "trace", skip_all, err)]
	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
	{
		/// The label [column](ContactColumns).
		const LABEL: &str = ContactColumns::default().label;

		fn write<'args, T>(s: &mut Separated<'_, 'args, Postgres, T>, c: &'args Contact)
		where
			T: Display,
		{
			s.push('(').push_unseparated(LABEL).push_unseparated('=').push_bind(&c.label).push_unseparated(')');
		}

		let mut peekable_entities = entities.peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		let mut query = QueryBuilder::new(sql::DELETE);
		query.push(sql::FROM).push(ContactColumns::TABLE_NAME).push(sql::WHERE);

		if let Some(e) = peekable_entities.next()
		{
			let mut separated = query.separated(' ');

			write(&mut separated, e);
			peekable_entities.for_each(|e| {
				separated.push_unseparated(sql::OR);
				write(&mut separated, e);
			});
		}

		tracing::debug!("Generated SQL: {}", query.sql());
		query.prepare().execute(connection).await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{address, contact, words};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{ContactAdapter, LocationAdapter},
		Deletable,
		Retrievable,
	};
	use winvoice_match::{MatchContact, MatchStr};
	use winvoice_schema::ContactKind;

	use crate::schema::{util, PgContact, PgLocation};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn delete()
	{
		let connection = util::connect();

		let country = PgLocation::create(&connection, None, address::country(), None).await.unwrap();

		let (office_number, primary_email, mailing_address) = futures::try_join!(
			PgContact::create(&connection, ContactKind::Phone(contact::phone()), words::sentence(3),),
			PgContact::create(&connection, ContactKind::Email(contact::email()), words::sentence(3)),
			PgContact::create(&connection, ContactKind::Address(country), words::sentence(3)),
		)
		.unwrap();

		PgContact::delete(&connection, [&office_number, &primary_email].into_iter()).await.unwrap();

		assert_eq!(
			PgContact::retrieve(&connection, MatchContact {
				label: MatchStr::from(office_number.label.clone()) |
					primary_email.label.clone().into() |
					mailing_address.label.clone().into(),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[mailing_address.clone()],
		);

		// cleanup for the test; since labels are the primary key
		PgContact::delete(&connection, [mailing_address].iter()).await.unwrap();
	}
}
