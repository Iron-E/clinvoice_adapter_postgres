use futures::TryStreamExt;
use sqlx::{Pool, Postgres, QueryBuilder, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::DepartmentColumns,
	Retrievable,
	WriteWhereClause,
};
use winvoice_match::MatchDepartment;
use winvoice_schema::Department;

use super::PgDepartment;
use crate::PgSchema;

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgDepartment
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Department;
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchDepartment;

	/// Retrieve all [`Department`]s (via `connection`) that match the `match_condition`.
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: DepartmentColumns<&'static str> = DepartmentColumns::default();

		let mut query = QueryBuilder::new(sql::SELECT);

		query.push_columns(&COLUMNS.default_scope()).push_default_from::<DepartmentColumns>();

		PgSchema::write_where_clause(
			Default::default(),
			DepartmentColumns::DEFAULT_ALIAS,
			&match_condition,
			&mut query,
		);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.map_ok(|row| Self::row_to_view(COLUMNS, &row))
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use winvoice_adapter::{schema::DepartmentAdapter, Retrievable};
	use winvoice_match::{Match, MatchDepartment, MatchStr};

	use crate::schema::{util, PgDepartment};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect();

		let (department, department2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
		)
		.unwrap();

		assert_eq!(
			PgDepartment::retrieve(&connection, MatchDepartment {
				id: Match::Or(
					[&department, &department2].into_iter().map(|d| d.id.into()).collect()
				),
				name: department.name.clone().into(),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[department],
		);

		assert_eq!(
			PgDepartment::retrieve(&connection, MatchStr::from(department2.name.clone()).into())
				.await
				.unwrap()
				.as_slice(),
			&[department2],
		);
	}
}
