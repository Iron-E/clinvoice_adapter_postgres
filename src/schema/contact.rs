mod contact_adapter;
mod deletable;
mod retrievable;
mod updatable;

use futures::TryFutureExt;
use sqlx::{postgres::PgRow, Error, Executor, Postgres, Result, Row};
use winvoice_adapter::schema::columns::ContactColumns;
use winvoice_schema::{Contact, ContactKind};

use super::PgLocation;

/// Implementor of the [`ContactAdapter`](winvoice_adapter::schema::ContactAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgContact;

impl PgContact
{
	/// Convert the `row` into a typed [`Contact`].
	pub async fn row_to_view<'connection, Conn>(
		connection: Conn,
		columns: ContactColumns<&str>,
		row: &PgRow,
	) -> Result<Contact>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		Ok(Contact {
			label: row.get(columns.label),
			kind: match row.get::<Option<_>, _>(columns.address_id)
			{
				Some(id) => PgLocation::retrieve_by_id(connection, id).map_ok(ContactKind::Address).await?,
				None => row
					.get::<Option<_>, _>(columns.email)
					.map(ContactKind::Email)
					.or_else(|| row.get::<Option<_>, _>(columns.other).map(ContactKind::Other))
					.or_else(|| row.get::<Option<_>, _>(columns.phone).map(ContactKind::Phone))
					.ok_or_else(|| {
						Error::Decode("Row of `contact_info` did not match any `Contact` equivalent".into())
					})?,
			},
		})
	}
}
