mod deletable;
mod organization_adapter;
mod retrievable;
mod updatable;

use sqlx::{postgres::PgRow, Executor, Postgres, Result, Row};
use winvoice_adapter::schema::columns::OrganizationColumns;
use winvoice_schema::Organization;

use super::PgLocation;

/// Implementor of the [`OrganizationAdapter`](winvoice_adapter::schema::OrganizationAdapter) for
/// the [`Postgres`](sqlx::Postgres) database.
pub struct PgOrganization;

impl PgOrganization
{
	pub async fn row_to_view<'connection, Conn, Column>(
		connection: Conn,
		columns: OrganizationColumns<Column>,
		row: &PgRow,
	) -> Result<Organization>
	where
		Conn: Executor<'connection, Database = Postgres>,
		Column: AsRef<str>,
	{
		let location_id = row.try_get(columns.location_id.as_ref())?;
		Ok(Organization {
			id: row.try_get(columns.id.as_ref())?,
			name: row.try_get(columns.name.as_ref())?,
			location: PgLocation::retrieve_by_id(connection, location_id).await?,
		})
	}
}
