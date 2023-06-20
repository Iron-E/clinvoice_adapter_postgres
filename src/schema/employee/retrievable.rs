use futures::TryStreamExt;
use sqlx::{Pool, Postgres, QueryBuilder, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::{DepartmentColumns, EmployeeColumns},
	Retrievable,
	WriteWhereClause,
};
use winvoice_match::MatchEmployee;
use winvoice_schema::Employee;

use super::PgEmployee;
use crate::PgSchema;

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgEmployee
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Employee;
	/// The type used for [match](winvoice_match)ing.
	type Match = MatchEmployee;

	/// Retrieve all [`Employee`]s (via `connection`) that match the `match_condition`.
	#[tracing::instrument(level = "trace", skip(connection), err)]
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: EmployeeColumns<&'static str> = EmployeeColumns::default();
		const DEPARTMENT_COLUMNS_UNIQUE: DepartmentColumns = DepartmentColumns::unique();

		let columns = COLUMNS.default_scope();
		let department_columns = DepartmentColumns::default().default_scope();
		let mut query = QueryBuilder::new(sql::SELECT);

		query
			.push_columns(&columns)
			.push_more_columns(&department_columns.r#as(DEPARTMENT_COLUMNS_UNIQUE))
			.push_default_from::<EmployeeColumns>()
			.push_default_equijoin::<DepartmentColumns, _, _>(
				department_columns.id,
				columns.department_id,
			);

		PgSchema::write_where_clause(
			PgSchema::write_where_clause(
				Default::default(),
				EmployeeColumns::DEFAULT_ALIAS,
				&match_condition,
				&mut query,
			),
			DepartmentColumns::DEFAULT_ALIAS,
			&match_condition.department,
			&mut query,
		);

		tracing::debug!("Generated SQL: {}", query.sql());
		query
			.prepare()
			.fetch(connection)
			.map_ok(|row| Self::row_to_view(COLUMNS, DEPARTMENT_COLUMNS_UNIQUE, &row))
			.try_collect()
			.await
	}
}

#[cfg(test)]
mod tests
{
	use std::collections::HashSet;

	use mockd::{job, name};
	use winvoice_adapter::{
		schema::{DepartmentAdapter, EmployeeAdapter},
		Retrievable,
	};
	use winvoice_match::{Match, MatchEmployee, MatchStr};

	use crate::schema::{util, PgDepartment, PgEmployee};

	#[tokio::test]
	#[tracing_test::traced_test]
	async fn retrieve()
	{
		let connection = util::connect().await;

		let (department, department2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
		)
		.unwrap();

		let (employee, employee2) = futures::try_join!(
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
			PgEmployee::create(&connection, department.clone(), name::full(), job::title()),
		)
		.unwrap();

		assert_eq!(
			PgEmployee::retrieve(&connection, MatchEmployee {
				department: department.id.into(),
				id: Match::Or([&employee, &employee2].into_iter().map(|e| e.id.into()).collect()),
				name: employee.name.clone().into(),
				..Default::default()
			})
			.await
			.unwrap()
			.as_slice(),
			&[employee.clone()],
		);

		assert_eq!(
			PgEmployee::retrieve(&connection, MatchEmployee {
				department: MatchStr::Or(
					[&department, &department2]
						.into_iter()
						.map(|e| e.name.clone().into())
						.collect()
				)
				.into(),
				id: Match::Or([&employee, &employee2].into_iter().map(|e| e.id.into()).collect()),
				name: MatchStr::Not(Box::new(util::different_string(employee.name.clone()).into())),
				..Default::default()
			})
			.await
			.unwrap()
			.into_iter()
			.collect::<HashSet<_>>(),
			[employee, employee2].into_iter().collect()
		);
	}
}
