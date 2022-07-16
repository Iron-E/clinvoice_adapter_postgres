use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::LocationColumns,
	Retrievable,
};
use clinvoice_match::MatchLocation;
use clinvoice_schema::Location;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Result, Row};

use super::PgLocation;
use crate::fmt::PgLocationRecursiveCte;

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgLocation
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Location;
	/// The type used for [match](clinvoice_match)ing.
	type Match = MatchLocation;

	/// Retrieve all [`Location`]s (via `connection`) that match the `match_condition`.
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: &Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: LocationColumns<&'static str> = LocationColumns::default();

		let mut query = Self::query_with_recursive(match_condition);

		query
			.push(sql::SELECT)
			.push(COLUMNS.default_scope().id)
			.push_from(
				PgLocationRecursiveCte::from(match_condition),
				LocationColumns::<char>::DEFAULT_ALIAS,
			)
			.prepare()
			.fetch(connection)
			.and_then(|row| PgLocation::retrieve_by_id(connection, row.get(COLUMNS.id)))
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use clinvoice_adapter::{schema::LocationAdapter, Retrievable};
	use clinvoice_match::{MatchLocation, MatchOuterLocation};
	use pretty_assertions::assert_eq;

	use crate::schema::{util, PgLocation};

	#[tokio::test]
	async fn retrieve()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let usa = PgLocation::create(&connection, "USA".into(), Some(earth.clone()))
			.await
			.unwrap();

		let (arizona, utah) = futures::try_join!(
			PgLocation::create(&connection, "Arizona".into(), Some(usa.clone())),
			PgLocation::create(&connection, "Utah".into(), Some(usa.clone())),
		)
		.unwrap();

		// Assert ::retrieve retrieves accurately from the DB
		assert_eq!(
			PgLocation::retrieve(&connection, &MatchLocation {
				id: earth.id.into(),
				outer: MatchOuterLocation::None,
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[earth]
		);

		assert_eq!(
			[utah, arizona].into_iter().collect::<HashSet<_>>(),
			PgLocation::retrieve(&connection, &MatchLocation {
				outer: MatchOuterLocation::Some(Box::new(MatchLocation {
					id: usa.id.into(),
					..Default::default()
				})),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>()
		);
	}
}
