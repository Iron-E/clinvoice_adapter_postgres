use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::OrganizationAdapter;
use winvoice_schema::{Id, Location, Organization};

use super::PgOrganization;

#[async_trait::async_trait]
impl OrganizationAdapter for PgOrganization
{
	async fn create<'connection, Conn>(connection: Conn, location: Location, name: String) -> Result<Organization>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let id = Id::new_v4();
		sqlx::query!("INSERT INTO organizations (id, location_id, name) VALUES ($1, $2, $3);", id, location.id, name)
			.execute(connection)
			.await?;

		Ok(Organization { id, location, name })
	}
}

#[cfg(test)]
mod tests
{
	use mockd::{address, company};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::schema::LocationAdapter;

	use super::{OrganizationAdapter, PgOrganization};
	use crate::schema::{util, PgLocation};

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect();

		let earth = PgLocation::create(&connection, None, address::street(), None).await.unwrap();

		let organization = PgOrganization::create(&connection, earth.clone(), company::company()).await.unwrap();

		let row = sqlx::query!("SELECT * FROM organizations WHERE id = $1;", organization.id)
			.fetch_one(&connection)
			.await
			.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(organization.id, row.id);
		assert_eq!(organization.location.id, earth.id);
		assert_eq!(organization.location.id, row.location_id);
		assert_eq!(organization.name, row.name);
	}
}
