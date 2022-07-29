//! # Summary
//!
//! This module implements adapters (and associated adapter types such as
//! [`Deletable`](clinvoice_adapter::Deletable)) for a Postgres filesystem.

mod contact;
mod employee;
mod expenses;
mod initializable;
mod job;
mod location;
mod organization;
mod timesheet;
mod util;
mod write_where_clause;

use clinvoice_adapter::{
	fmt::{sql, As, ColumnsToSql, QueryBuilderExt, SnakeCase, TableToSql},
	WriteWhereClause,
};
use clinvoice_match::Match;
use clinvoice_schema::Id;
pub use contact::PgContact;
pub use employee::PgEmployee;
pub use expenses::PgExpenses;
pub use job::PgJob;
pub use location::PgLocation;
pub use organization::PgOrganization;
use sqlx::{Executor, Postgres, QueryBuilder, Result, Transaction};
pub use timesheet::PgTimesheet;

/// The struct which implements several [`clinvoice_adapter`] traits to allow CLInvoice to function
/// within a Postgres database environment.
pub struct PgSchema;

impl PgSchema
{
	/// Via `connection`, execute `DELETE FROM {table} WHERE (id = №) OR … OR (id = №)` for each
	/// [`Id`] in `ids`.
	async fn delete<'args, Conn, Iter, Table>(connection: Conn, ids: Iter) -> Result<()>
	where
		Conn: Executor<'args, Database = Postgres>,
		Iter: Iterator<Item = Id>,
		Table: TableToSql,
	{
		let mut peekable_entities = ids.peekable();

		// There is nothing to do
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		let mut query = QueryBuilder::new(sql::DELETE);
		query.push(sql::FROM).push(Table::TABLE_NAME);

		Self::write_where_clause(
			Default::default(),
			"id",
			&Match::Or(peekable_entities.map(Match::from).collect()),
			&mut query,
		);

		query.prepare().execute(connection).await?;

		Ok(())
	}

	/// Execute a query over the given `connection` which updates `columns` of a `table` given
	/// the some values specified by `push_values` (e.g.
	/// `|query| query.push_values(my_iterator, |mut q, value| …)`).
	///
	/// # See also
	///
	/// * [`ColumnsToSql::push_columns`] for how the order of columns to bind in `push_values`.
	/// * [`ColumnsToSql::push_set`] for how the `SET` clause is generated.
	/// * [`ColumnsToSql::push_update_where`] for how the `WHERE` condition is generated.
	/// * [`QueryBuilder::push_values`] for what function to use for `push_values`.
	async fn update<'args, Columns, F>(
		connection: &mut Transaction<'_, Postgres>,
		columns: Columns,
		push_values: F,
	) -> Result<()>
	where
		Columns: ColumnsToSql,
		F: FnOnce(&mut QueryBuilder<'args, Postgres>),
	{
		let mut query = QueryBuilder::new(sql::UPDATE);

		query
			.push(As(Columns::TABLE_NAME, Columns::DEFAULT_ALIAS))
			.push(sql::SET);

		let values_alias = SnakeCase::from((Columns::DEFAULT_ALIAS, 'V'));
		columns.push_set_to(&mut query, values_alias);

		query.push(sql::FROM).push('(');

		push_values(&mut query);

		query
			.push(')')
			.push(sql::AS)
			.push(values_alias)
			.push(" (")
			.push_columns(&columns)
			.push(')')
			.push(sql::WHERE);

		columns.push_update_where_to(&mut query, Columns::DEFAULT_ALIAS, values_alias);

		query.prepare().execute(connection).await?;

		Ok(())
	}
}
