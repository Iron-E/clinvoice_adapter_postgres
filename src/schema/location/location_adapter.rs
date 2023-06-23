use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::schema::LocationAdapter;
use winvoice_schema::{Currency, Id, Location};

use super::PgLocation;

#[async_trait::async_trait]
impl LocationAdapter for PgLocation
{
	async fn create<'connection, Conn>(
		connection: Conn,
		currency: Option<Currency>,
		name: String,
		outer: Option<Location>,
	) -> Result<Location>
	where
		Conn: Executor<'connection, Database = Postgres>,
	{
		let id = Id::new_v4();
		sqlx::query!(
			"INSERT INTO locations (id, currency, name, outer_id) VALUES ($1, $2, $3, $4);",
			id,
			currency.map(|c| -> &str { c.into() }),
			name,
			outer.as_ref().map(|o| o.id)
		)
		.execute(connection)
		.await?;

		Ok(Location { currency, id, name, outer: outer.map(Into::into) })
	}
}

#[cfg(test)]
mod tests
{
	use mockd::address;
	use pretty_assertions::assert_eq;

	use super::{Currency, LocationAdapter, PgLocation};
	use crate::schema::util;

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::city(), None).await.unwrap();

		let street = PgLocation::create(
			&connection,
			Currency::Usd.into(),
			util::rand_street_name(),
			city.clone().into(),
		)
		.await
		.unwrap();

		let (location, location2) = futures::try_join!(
			PgLocation::create(&connection, None, address::street_name(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_name(), street.clone().into()),
		)
		.unwrap();

		macro_rules! select {
			($id:expr) => {
				sqlx::query!("SELECT * FROM locations WHERE id = $1", $id)
					.fetch_one(&connection)
					.await
					.unwrap()
			};
		}

		// Assert ::create writes accurately to the DB
		let database_city = select!(city.id);
		assert_eq!(city.currency.map(|c| c.to_string()), database_city.currency);
		assert_eq!(city.id, database_city.id);
		assert_eq!(city.name, database_city.name);
		assert_eq!(city.outer, None);
		assert_eq!(city.outer.map(|o| o.id), database_city.outer_id);

		// Assert ::create_inner writes accurately to the DB when `outer_id` is `None`
		let database_street = select!(street.id);
		assert_eq!(street.currency.map(|c| c.to_string()), database_street.currency);
		assert_eq!(street.id, database_street.id);
		assert_eq!(street.name, database_street.name);
		let street_outer_id = street.outer.map(|o| o.id);
		assert_eq!(street_outer_id, Some(city.id));
		assert_eq!(street_outer_id, database_street.outer_id);

		// Assert ::create_inner writes accurately to the DB when `outer_id` is `Some(…)`
		let database_location = select!(location.id);
		assert_eq!(location.currency.map(|c| c.to_string()), database_location.currency);
		assert_eq!(location.id, database_location.id);
		assert_eq!(location.name, database_location.name);
		let location_outer_id = location.outer.map(|o| o.id);
		assert_eq!(location_outer_id, Some(street.id));
		assert_eq!(location_outer_id, database_location.outer_id);

		let database_location2 = select!(location2.id);
		assert_eq!(location2.currency.map(|c| c.to_string()), database_location2.currency);
		assert_eq!(location2.id, database_location2.id);
		assert_eq!(location2.name, database_location2.name);
		let location2_outer_id = location2.outer.map(|o| o.id);
		assert_eq!(location2_outer_id, Some(street.id));
		assert_eq!(location2_outer_id, database_location2.outer_id);
	}
}
