use core::fmt::Display;

use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::ContactColumns,
	Deletable,
};
use clinvoice_schema::Contact;
use sqlx::{query_builder::Separated, Executor, Postgres, QueryBuilder, Result};

use super::PgContact;

#[async_trait::async_trait]
impl Deletable for PgContact
{
	type Db = Postgres;
	type Entity = Contact;

	async fn delete<'c, 'e, 'i, TConn, TIter>(connection: TConn, entities: TIter) -> Result<()>
	where
		'e: 'i,
		Self::Entity: 'e,
		TConn: Executor<'c, Database = Self::Db>,
		TIter: Iterator<Item = &'i Self::Entity> + Send,
	{
		fn write<'query, 'args, T>(s: &mut Separated<'query, 'args, Postgres, T>, c: &'args Contact)
		where
			T: Display,
		{
			s.push('(')
				.push_unseparated(ContactColumns::default().label)
				.push_unseparated('=')
				.push_bind(&c.label)
				.push_unseparated(')');
		}

		let mut peekable_entities = entities.peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		let mut query = QueryBuilder::new(sql::DELETE);
		query
			.push(sql::FROM)
			.push(ContactColumns::<&str>::TABLE_NAME)
			.push(sql::WHERE);

		{
			let mut separated = query.separated(' ');

			if let Some(e) = peekable_entities.next()
			{
				write(&mut separated, e);
			}

			peekable_entities.for_each(|e| {
				separated.push_unseparated(sql::OR);
				write(&mut separated, e);
			});
		}

		query.prepare().execute(connection).await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests
{
	use clinvoice_adapter::{
		schema::{ContactAdapter, LocationAdapter},
		Deletable,
		Retrievable,
	};
	use clinvoice_match::{MatchContact, MatchStr};
	use clinvoice_schema::ContactKind;
	use pretty_assertions::assert_eq;

	use crate::schema::{util, PgContact, PgLocation};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let (office_number, primary_email, mailing_address) = futures::try_join!(
			PgContact::create(
				&connection,
				ContactKind::Phone("555-555-5555".into()),
				"Office Number".into()
			),
			PgContact::create(
				&connection,
				ContactKind::Email("somethingsomething@invalid.com".into()),
				"Primary Email".into()
			),
			PgContact::create(
				&connection,
				ContactKind::Address(earth),
				"Mailing Address".into()
			),
		)
		.unwrap();

		PgContact::delete(&connection, [&office_number, &primary_email].into_iter())
			.await
			.unwrap();

		assert_eq!(
			PgContact::retrieve(&connection, &MatchContact {
				label: MatchStr::Or(vec![
					office_number.label.clone().into(),
					primary_email.label.clone().into(),
					mailing_address.label.clone().into(),
				]),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[mailing_address.clone()],
		);

		// cleanup for the test; since labels are the primary key
		PgContact::delete(&connection, [mailing_address].iter())
			.await
			.unwrap();
	}
}
