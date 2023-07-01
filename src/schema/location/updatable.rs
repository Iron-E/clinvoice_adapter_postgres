use core::cmp::Ordering;

use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::LocationColumns, Updatable};
use winvoice_schema::Location;

use super::PgLocation;
use crate::PgSchema;

#[async_trait::async_trait]
impl Updatable for PgLocation
{
	type Db = Postgres;
	type Entity = Location;

	async fn update<'entity, Iter>(connection: &mut Transaction<Self::Db>, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		let mut entities_peekable = entities.peekable();

		// There is nothing to do.
		if entities_peekable.peek().is_none()
		{
			return Ok(());
		}

		let mut entities_collected: Vec<_> = entities_peekable.collect();
		{
			let mut idx = 0;

			loop
			{
				let mut outers = entities_collected[idx..].iter().filter_map(|e| e.outer.as_deref()).peekable();

				// There are no more outer locations, so we can stop looking for them in this loop.
				if outers.peek().is_none()
				{
					break;
				}

				let outers_collected: Vec<_> = outers.collect();

				entities_collected.extend(outers_collected);
				idx = entities_collected.len();
			}
		}

		// NOTE: we don't want to update a given row in the DB more than once.
		// PERF: we can only get duplicates if there is more than one entitiy to update.
		if entities_collected.len() > 1
		{
			// PERF: `dedup` needs a list to be sorted. there's no way for two duplicates to get
			//       unsorted unless there are at least three elements.
			if entities_collected.len() > 2
			{
				entities_collected.sort_by(|lhs, rhs| match rhs.id.cmp(&lhs.id)
				{
					// NOTE: this allows `dedup_by_key` prune edits to `Location`s which would
					// overwrite       the `Location`s which were initially passed to the function
					// (e.g. if street       and Sweden are both passed in to this function, street
					// will take precedence       over Sweden's copy of street).
					Ordering::Equal => Ordering::Greater,
					o => o,
				});
			}

			entities_collected.dedup_by_key(|e| e.id);
		}

		PgSchema::update(connection, LocationColumns::default(), |query| {
			query.push_values(entities_collected.iter(), |mut q, e| {
				q.push_bind(e.currency.map(|c| -> &str { c.into() }))
					.push_bind(e.id)
					.push_bind(&e.name)
					.push_bind(e.outer.as_ref().map(|o| o.id));
			});
		})
		.await
	}
}

#[cfg(test)]
mod tests
{
	use mockd::address;
	use pretty_assertions::{assert_eq, assert_ne};
	use winvoice_adapter::{schema::LocationAdapter, Retrievable, Updatable};
	use winvoice_schema::Location;

	use crate::schema::{util, PgLocation};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect();

		let mut street = PgLocation::create(&connection, None, util::rand_street_name(), None).await.unwrap();

		let (mut location, mut location2) = futures::try_join!(
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
		)
		.unwrap();

		// NOTE: creating this location last to make sure that new locations can be set outside of
		//       old locations
		let street2 = PgLocation::create(&connection, None, util::rand_street_name(), None).await.unwrap();

		location.name = util::different_string(&location.name);
		location.outer =
			Some(Location { id: street.id, name: util::different_string(&street.name), ..Default::default() }.into());
		street.name = util::different_string(&street.name);

		location2.outer = Some(street2.into());

		{
			let mut tx = connection.begin().await.unwrap();
			PgLocation::update(&mut tx, [&location, &location2, &street].into_iter()).await.unwrap();
			tx.commit().await.unwrap();
		}

		let location_db = PgLocation::retrieve(&connection, location.id.into()).await.unwrap().pop().unwrap();

		let location2_db = PgLocation::retrieve(&connection, location2.id.into()).await.unwrap().pop().unwrap();

		assert_eq!(location.id, location_db.id);
		assert_eq!(location.name, location_db.name);
		assert_ne!(location.outer, location_db.outer);
		assert_eq!(street, *location_db.outer.unwrap());

		assert_eq!(location2, location2_db);
	}
}
