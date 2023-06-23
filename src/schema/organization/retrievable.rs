use futures::TryStreamExt;
use sqlx::{Pool, Postgres, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{LocationColumns, OrganizationColumns},
	Retrievable,
	WriteWhereClause,
};
use winvoice_match::MatchOrganization;
use winvoice_schema::Organization;

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
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchOrganization;

	/// Retrieve all [`Organization`]s (via `connection`) that match the `match_condition`.
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: OrganizationColumns<&'static str> = OrganizationColumns::default();

		let columns = COLUMNS.default_scope();
		let location_columns = LocationColumns::default().default_scope();
		let mut query = PgLocation::query_with_recursive(&match_condition.location);

		query
			.push(sql::SELECT)
			.push_columns(&columns)
			.push_default_from::<OrganizationColumns>()
			.push_equijoin(
				PgLocationRecursiveCte::from(&match_condition.location),
				LocationColumns::DEFAULT_ALIAS,
				location_columns.id,
				columns.location_id,
			);

		PgSchema::write_where_clause(
			Default::default(),
			OrganizationColumns::DEFAULT_ALIAS,
			&match_condition,
			&mut query,
		);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.and_then(|row| async move { Self::row_to_view(connection, COLUMNS, &row).await })
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use mockd::{address, company};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{LocationAdapter, OrganizationAdapter},
		Retrievable,
	};
	use winvoice_match::{Match, MatchLocation, MatchOrganization};
	use winvoice_schema::Id;

	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect();

		let city = PgLocation::create(&connection, None, address::city(), None).await.unwrap();
		let street = PgLocation::create(&connection, None, util::rand_street_name(), city.into())
			.await
			.unwrap();

		let (location, location2) = futures::try_join!(
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
			PgLocation::create(&connection, None, address::street_number(), street.clone().into()),
		)
		.unwrap();

		let (organization, organization2) = futures::try_join!(
			PgOrganization::create(&connection, location.clone(), company::company()),
			PgOrganization::create(&connection, location2, company::company()),
		)
		.unwrap();

		// Assert ::retrieve gets the right data from the DB
		assert_eq!(
			PgOrganization::retrieve(&connection, organization.id.into()).await.unwrap().as_slice(),
			&[organization.clone()],
		);

		assert_eq!(
			PgOrganization::retrieve(&connection, MatchOrganization {
				location: MatchLocation {
					outer: Some(
						MatchLocation {
							id: Match::Or(vec![street.id.into(), Id::new_v4().into()]),
							name: street.name.into(),
							..Default::default()
						}
						.into()
					)
					.into(),
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
