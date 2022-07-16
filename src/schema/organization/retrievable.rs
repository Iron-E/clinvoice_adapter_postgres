use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{LocationColumns, OrganizationColumns},
	Retrievable,
	WriteWhereClause,
};
use clinvoice_match::MatchOrganization;
use clinvoice_schema::Organization;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Result};

use super::PgOrganization;
use crate::{fmt::PgLocationRecursiveCte, schema::PgLocation, PgSchema};

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgOrganization
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Organization;
	/// The type used for [match](clinvoice_match)ing.
	type Match = MatchOrganization;

	/// Retrieve all [`Organization`]s (via `connection`) that match the `match_condition`.
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: &Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: OrganizationColumns<&'static str> = OrganizationColumns::default();

		let columns = COLUMNS.default_scope();
		let location_columns = LocationColumns::default().default_scope();
		let mut query = PgLocation::query_with_recursive(&match_condition.location);

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_default_from::<OrganizationColumns<char>>()
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_condition.location),
				LocationColumns::<char>::DEFAULT_ALIAS,
				location_columns.id,
				columns.location_id,
			);

		PgSchema::write_where_clause(
			Default::default(),
			OrganizationColumns::<char>::DEFAULT_ALIAS,
			match_condition,
			&mut query,
		);

		query
			.prepare()
			.fetch(connection)
			.and_then(
				|row| async move { PgOrganization::row_to_view(connection, COLUMNS, &row).await },
			)
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use clinvoice_adapter::{
		schema::{LocationAdapter, OrganizationAdapter},
		Retrievable,
	};
	use clinvoice_match::{Match, MatchLocation, MatchOrganization, MatchOuterLocation};
	use pretty_assertions::assert_eq;

	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	async fn retrieve()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let usa = PgLocation::create(&connection, "USA".into(), Some(earth))
			.await
			.unwrap();

		let (arizona, utah) = futures::try_join!(
			PgLocation::create(&connection, "Arizona".into(), Some(usa.clone())),
			PgLocation::create(&connection, "Utah".into(), Some(usa.clone())),
		)
		.unwrap();

		let (organization, organization2) = futures::try_join!(
			PgOrganization::create(&connection, arizona.clone(), "Some Organization".into()),
			PgOrganization::create(&connection, utah, "Some Other Organizatión".into()),
		)
		.unwrap();

		// Assert ::retrieve gets the right data from the DB
		assert_eq!(
			PgOrganization::retrieve(&connection, &MatchOrganization {
				id: organization.id.into(),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[organization.clone()],
		);

		assert_eq!(
			PgOrganization::retrieve(&connection, &MatchOrganization {
				location: MatchLocation {
					outer: MatchOuterLocation::Some(
						MatchLocation {
							id: Match::InRange(usa.id - 1, usa.id + 1),
							name: usa.name.into(),
							..Default::default()
						}
						.into()
					),
					..Default::default()
				},
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			[organization, organization2].into_iter().collect(),
		);
	}
}
