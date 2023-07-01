use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::ContactAdapter;
use winvoice_schema::{Contact, ContactKind};

use super::PgContact;

#[async_trait::async_trait]
impl ContactAdapter for PgContact
{
	async fn create<'connection, Conn>(connection: Conn, kind: ContactKind, label: String) -> Result<Contact>
	where
		Conn: Executor<'connection, Database = Postgres>,
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
}
