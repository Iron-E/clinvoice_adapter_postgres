use clinvoice_adapter::schema::OrganizationAdapter;
use clinvoice_schema::{Location, Organization};
use sqlx::{Executor, Postgres, Result};

use super::PgOrganization;

#[async_trait::async_trait]
impl OrganizationAdapter for PgOrganization
{
	async fn create<'connection, Conn>(
		connection: Conn,
		location: Location,
		name: String,
	) -> Result<Organization>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let row = sqlx::query!(
			"INSERT INTO organizations (location_id, name) VALUES ($1, $2) RETURNING id;",
			location.id,
			name
		)
		.fetch_one(connection)
		.await?;

		Ok(Organization {
			id: row.id,
			location,
			name,
		})
	}
}

#[cfg(test)]
mod tests
{
	use clinvoice_adapter::schema::LocationAdapter;
	use pretty_assertions::assert_eq;

	use super::{OrganizationAdapter, PgOrganization};
	use crate::schema::{util, PgLocation};

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let organization =
			PgOrganization::create(&connection, earth.clone(), "Some Organization".into())
				.await
				.unwrap();

		let row = sqlx::query!(
			"SELECT * FROM organizations WHERE id = $1;",
			organization.id
		)
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
