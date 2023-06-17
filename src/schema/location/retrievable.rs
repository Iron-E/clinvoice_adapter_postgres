use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Result, Row};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::LocationColumns,
	Retrievable,
};
use winvoice_match::MatchLocation;
use winvoice_schema::Location;

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
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchLocation;

	/// Retrieve all [`Location`]s (via `connection`) that match the `match_condition`.
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: LocationColumns<&'static str> = LocationColumns::default();

		let mut query = Self::query_with_recursive(&match_condition);

		query.push(sql::SELECT).push(COLUMNS.default_scope().id).push_from(
			PgLocationRecursiveCte::from(&match_condition),
			LocationColumns::DEFAULT_ALIAS,
		);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.and_then(|row| Self::retrieve_by_id(connection, row.get(COLUMNS.id)))
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use pretty_assertions::assert_eq;
	use winvoice_adapter::{schema::LocationAdapter, Retrievable};
	use winvoice_match::MatchLocation;

	use crate::schema::{util, PgLocation};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, None, "Earth".into(), None).await.unwrap();

		let usa = PgLocation::create(&connection, None, "USA".into(), earth.clone().into())
			.await
			.unwrap();

		let (arizona, utah) = futures::try_join!(
			PgLocation::create(&connection, None, "Arizona".into(), usa.clone().into()),
			PgLocation::create(&connection, None, "Utah".into(), usa.clone().into()),
		)
		.unwrap();

		// Assert ::retrieve retrieves accurately from the DB
		assert_eq!(
			PgLocation::retrieve(&connection, MatchLocation {
				id: earth.id.into(),
				outer: None.into(),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[earth]
		);

		assert_eq!(
			[utah, arizona].into_iter().collect::<HashSet<_>>(),
			PgLocation::retrieve(&connection, MatchLocation {
				outer: Some(Box::new(usa.id.into())).into(),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>()
		);
	}
}
