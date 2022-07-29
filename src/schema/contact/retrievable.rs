use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::ContactColumns,
	Retrievable,
};
use clinvoice_match::MatchContact;
use clinvoice_schema::Contact;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, QueryBuilder, Result};

use super::PgContact;
use crate::schema::write_where_clause;

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgContact
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Contact;
	/// The type used for [match](clinvoice_match)ing.
	type Match = MatchContact;

	/// Retrieve all [`Contact`]s (via `connection`) that match the `match_condition`.
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: &Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: ContactColumns<&'static str> = ContactColumns::default();

		let mut query = QueryBuilder::new(sql::SELECT);

		query
			.push_columns(&COLUMNS.default_scope())
			.push_default_from::<ContactColumns<char>>();

		write_where_clause::write_match_contact(
			connection,
			Default::default(),
			ContactColumns::<char>::DEFAULT_ALIAS,
			match_condition,
			&mut query,
		)
		.await?;

		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move { Self::row_to_view(connection, COLUMNS, &row).await })
			.try_collect()
			.await
	}
}
