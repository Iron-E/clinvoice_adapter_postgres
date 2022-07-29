use clinvoice_adapter::schema::ContactAdapter;
use clinvoice_schema::{Contact, ContactKind};
use sqlx::{Executor, Postgres, Result};

use super::PgContact;

#[async_trait::async_trait]
impl ContactAdapter for PgContact
{
	async fn create<'connection, TConn>(
		connection: TConn,
		kind: ContactKind,
		label: String,
	) -> Result<Contact>
	where
		TConn: Executor<'connection, Database = Postgres>,
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
