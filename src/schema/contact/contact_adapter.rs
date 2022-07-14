use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::{columns::ContactColumns, ContactAdapter},
};
use clinvoice_match::MatchContact;
use clinvoice_schema::{Contact, ContactKind};
use futures::TryStreamExt;
use sqlx::{Executor, PgPool, Postgres, QueryBuilder, Result};

use super::PgContact;
use crate::schema::write_where_clause;

#[async_trait::async_trait]
impl ContactAdapter for PgContact
{
	async fn create(
		connection: impl 'async_trait + Executor<'_, Database = Postgres> + Send,
		kind: ContactKind,
		label: String,
	) -> Result<Contact>
	{
		sqlx::query!(
			"INSERT INTO contact_information (address_id, email, label, other, phone)
			VALUES ($1, $2, $3, $4, $5);",
			kind.address().map(|a| a.id),
			kind.email(),
			&label,
			kind.other(),
			kind.phone(),
		)
		.execute(connection)
		.await?;

		Ok(Contact { kind, label })
	}

	async fn retrieve(connection: &PgPool, match_condition: &MatchContact) -> Result<Vec<Contact>>
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
			.and_then(|row| async move { PgContact::row_to_view(connection, COLUMNS, &row).await })
			.try_collect()
			.await
	}
}
