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
	async fn delete<'connection, 'entity, Conn, Iter>(
		connection: Conn,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		/// The label [column](ContactColumns).
		const LABEL: &'static str = ContactColumns::default().label;

		fn write<'args, T>(s: &mut Separated<'_, 'args, Postgres, T>, c: &'args Contact)
		where
			T: Display,
		{
			s.push('(')
				.push_unseparated(LABEL)
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
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, None, "Earth".into(), None).await.unwrap();

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
			PgContact::create(&connection, ContactKind::Address(earth), "Mailing Address".into()),
		)
		.unwrap();

		PgContact::delete(&connection, [&office_number, &primary_email].into_iter()).await.unwrap();

		assert_eq!(
			PgContact::retrieve(&connection, MatchContact {
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
		PgContact::delete(&connection, [mailing_address].iter()).await.unwrap();
	}
}
