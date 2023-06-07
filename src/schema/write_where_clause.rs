//! This module provides tools to write SQL `where` clauses using [`sqlx`] and [`TableToSql`].

use core::{fmt::Display, ops::Deref};

use sqlx::{Database, Executor, Postgres, QueryBuilder, Result};
use winvoice_adapter::{
	fmt::{sql, QueryBuilderExt, SnakeCase, TableToSql},
	schema::columns::{
		ContactColumns,
		EmployeeColumns,
		ExpenseColumns,
		JobColumns,
		OrganizationColumns,
		TimesheetColumns,
	},
	WriteContext,
	WriteWhereClause,
};
use winvoice_match::{
	Match,
	MatchContact,
	MatchContactKind,
	MatchEmployee,
	MatchExpense,
	MatchInvoice,
	MatchJob,
	MatchOption,
	MatchOrganization,
	MatchSet,
	MatchStr,
	MatchTimesheet,
};

use super::{PgLocation, PgSchema};
use crate::fmt::{PgContains, PgInterval, PgTimestampTz};

/// Write [`Match::Any`], [`MatchStr::Any`], [`MatchOption::Any`], or [`MatchSet::Any`] in a way
/// that will produce valid syntax.
fn write_any<Db>(query: &mut QueryBuilder<Db>, context: WriteContext)
where
	Db: Database,
{
	query.push(context).push(sql::TRUE);
}

/// Append `"{context} ("` to `query`. If `NEGATE` is `true`, append `"{context} NOT ("`.
///
/// # See also
///
/// * [`write_context_scope_end`]
fn write_context_scope_start<Db, const NEGATE: bool>(
	query: &mut QueryBuilder<Db>,
	context: WriteContext,
) where
	Db: Database,
{
	query.push(context);
	if NEGATE
	{
		query.push(sql::NOT);
	}
	query.push(" (");
}

/// Write `')'` to the `query`.
///
/// # See also
///
/// * [`write_context_scope_start`]
fn write_context_scope_end<Db>(query: &mut QueryBuilder<Db>)
where
	Db: Database,
{
	query.push(')');
}

/// Write multiple `AND`/`OR` `conditions`.
///
/// * If `UNION` is `true`, the `conditions` are separated by `AND`: `[Match::EqualTo(3),
///   Match::LessThan(4)]` is interpreted as `(foo = 3 AND foo < 4)`.
/// * If `UNION` is `false`, the `conditions` are separated by `OR`: `[Match::EqualTo(3),
///   Match::LessThan(4)]` is interpreted as `(foo = 3 OR foo < 4)`.
///
/// The rest of the args are the same as [`WriteSql::write_where`].
///
/// # Errors
///
/// If any the following:
///
/// * `ident` is empty.
fn write_boolean_group<Db, Ident, Iter, Match, const UNION: bool>(
	query: &mut QueryBuilder<Db>,
	context: WriteContext,
	ident: Ident,
	conditions: &mut Iter,
) where
	Ident: Copy + Display,
	Db: Database,
	Iter: Iterator<Item = Match>,
	PgSchema: WriteWhereClause<Db, Match>,
{
	write_context_scope_start::<_, false>(query, context);

	if let Some(m) = conditions.next()
	{
		PgSchema::write_where_clause(WriteContext::InWhereCondition, ident, m, query);
	}

	let separator = if UNION { sql::AND } else { sql::OR };
	conditions.for_each(|c| {
		query.push(separator);
		PgSchema::write_where_clause(WriteContext::InWhereCondition, ident, c, query);
	});

	write_context_scope_end(query);
}

/// Write a comparison of `ident` and `comparand` using `comparator`.
///
/// The rest of the args are the same as [`WriteSql::write_where`].
///
/// # Errors
///
/// If any the following:
///
/// * `ident` is empty.
///
/// # Warnings
///
/// * Does not guard against SQL injection.
fn write_comparison<Db, Ident, Comparand>(
	query: &mut QueryBuilder<Db>,
	context: WriteContext,
	ident: Ident,
	comparator: &str,
	comparand: Comparand,
) where
	Db: Database,
	Ident: Copy + Display,
	Comparand: Copy + Display,
{
	query.separated(' ').push(context).push(ident).push(comparator).push(comparand);
}

/// An implementation of [`WriteWhereClause`] for [`MatchContact`].
///
/// Must be `async` because it involves multiple intermediary database queries to accomplish.
///
/// # Errors
///
/// If any the following:
///
/// * `ident` is empty.
///
/// # See also
///
/// * [`WriteWhereClause::write_where_clause`].
#[tracing::instrument(level = "trace", skip_all)]
pub(super) async fn write_match_contact<'connection, Conn, Ident>(
	connection: Conn,
	context: WriteContext,
	ident: Ident,
	match_condition: &MatchContact,
	query: &mut QueryBuilder<'_, Postgres>,
) -> Result<WriteContext>
where
	Conn: Executor<'connection, Database = Postgres>,
	Ident: Copy + Display + Send + Sync,
{
	let columns = ContactColumns::default().scope(ident);

	let ctx = PgSchema::write_where_clause(context, columns.label, &match_condition.label, query);
	match match_condition.kind
	{
		MatchContactKind::Any => write_any(query, ctx),

		MatchContactKind::Address(ref location) =>
		{
			let location_id_query = PgLocation::retrieve_matching_ids(connection, location).await?;
			PgSchema::write_where_clause(ctx, columns.address_id, &location_id_query, query);
		},

		MatchContactKind::Email(ref email_address) =>
		{
			PgSchema::write_where_clause(ctx, columns.email, email_address, query);
		},

		MatchContactKind::Other(ref other) =>
		{
			PgSchema::write_where_clause(ctx, columns.other, other, query);
		},

		MatchContactKind::Phone(ref phone_number) =>
		{
			PgSchema::write_where_clause(ctx, columns.phone, phone_number, query);
		},
	};

	tracing::trace!("After: {}", query.sql());
	Ok(WriteContext::AcceptingAnotherWhereCondition)
}

/// Append `"{context} NOT ({match_condition})"` to the `query`.
///
/// The args are the same as [`WriteSql::write_where`].
fn write_negated<Db, Ident, Match>(
	query: &mut QueryBuilder<Db>,
	context: WriteContext,
	ident: Ident,
	match_condition: Match,
) where
	Db: Database,
	Ident: Copy + Display,
	PgSchema: WriteWhereClause<Db, Match>,
{
	write_context_scope_start::<_, true>(query, context);

	PgSchema::write_where_clause(WriteContext::InWhereCondition, ident, match_condition, query);

	write_context_scope_end(query);
}

impl<T> WriteWhereClause<Postgres, &Match<T>> for PgSchema
where
	T: Display + PartialEq,
{
	#[tracing::instrument(name = "write Match<T> clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &Match<T>,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		match match_condition
		{
			Match::And(conditions) => write_boolean_group::<_, _, _, _, true>(
				query,
				context,
				ident,
				&mut conditions.iter().filter(|m| Match::Any.eq(*m)),
			),
			Match::Any => write_any(query, context),
			Match::EqualTo(value) => write_comparison(query, context, ident, "=", value),
			Match::GreaterThan(value) => write_comparison(query, context, ident, ">", value),
			Match::InRange(low, high) =>
			{
				write_comparison(query, context, ident, sql::BETWEEN, low);
				write_comparison(query, WriteContext::InWhereCondition, "", sql::AND, high);
			},
			Match::LessThan(value) => write_comparison(query, context, ident, "<", value),
			Match::Not(condition) => write_negated(query, context, ident, condition.deref()),
			Match::Or(conditions) => write_boolean_group::<_, _, _, _, false>(
				query,
				context,
				ident,
				&mut conditions.iter().filter(|m| Match::Any.eq(*m)),
			),
		};

		tracing::trace!("After: {}", query.sql());
		WriteContext::AcceptingAnotherWhereCondition
	}
}

/// Implement [`WriteWhereClause`] for [`MatchOption`]
macro_rules! impl_write_where_clause_for_match_option {
	($Match:ty) => {
		impl WriteWhereClause<Postgres, &MatchOption<$Match>> for PgSchema
		{
			#[tracing::instrument(
				name = "Write MatchOption<$Match> clause",
				level = "trace",
				skip_all
			)]
			fn write_where_clause<Ident>(
				context: WriteContext,
				ident: Ident,
				match_condition: &MatchOption<$Match>,
				query: &mut QueryBuilder<Postgres>,
			) -> WriteContext
			where
				Ident: Copy + Display,
			{
				match match_condition
				{
					MatchOption::Any => write_any(query, context),
					MatchOption::None =>
					{
						query
							.separated(' ')
							.push(context)
							.push(ident)
							.push_unseparated(sql::IS)
							.push_unseparated(sql::NULL);
					},
					MatchOption::Some(condition) =>
					{
						PgSchema::write_where_clause(context, ident, condition, query);
					},
				};

				tracing::trace!("After: {}", query.sql());
				WriteContext::AcceptingAnotherWhereCondition
			}
		}
	};

	($Match:ident[T]) => {
		impl<T> WriteWhereClause<Postgres, &MatchOption<$Match<T>>> for PgSchema
		where
			T: Display + PartialEq,
		{
			#[tracing::instrument(
				name = "Write MatchOption<$Match> clause",
				level = "trace",
				skip_all
			)]
			fn write_where_clause<Ident>(
				context: WriteContext,
				ident: Ident,
				match_condition: &MatchOption<$Match<T>>,
				query: &mut QueryBuilder<Postgres>,
			) -> WriteContext
			where
				Ident: Copy + Display,
			{
				match match_condition
				{
					MatchOption::Any => write_any(query, context),
					MatchOption::None =>
					{
						query
							.separated(' ')
							.push(context)
							.push(ident)
							.push_unseparated(sql::IS)
							.push_unseparated(sql::NULL);
					},
					MatchOption::Some(condition) =>
					{
						PgSchema::write_where_clause(context, ident, condition, query);
					},
				};

				tracing::trace!("After: {}", query.sql());
				WriteContext::AcceptingAnotherWhereCondition
			}
		}
	};
}

impl_write_where_clause_for_match_option!(Match[T]);
impl_write_where_clause_for_match_option!(MatchEmployee);
impl_write_where_clause_for_match_option!(MatchExpense);
impl_write_where_clause_for_match_option!(MatchInvoice);
impl_write_where_clause_for_match_option!(MatchJob);
impl_write_where_clause_for_match_option!(MatchOrganization);
impl_write_where_clause_for_match_option!(MatchSet<MatchExpense>);
impl_write_where_clause_for_match_option!(MatchStr<String>);
impl_write_where_clause_for_match_option!(MatchTimesheet);

impl WriteWhereClause<Postgres, &MatchSet<MatchExpense>> for PgSchema
{
	#[tracing::instrument(name = "write MatchSet<MatchExpense> clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchSet<MatchExpense>,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		match match_condition
		{
			MatchSet::Any => write_any(query, context),

			MatchSet::And(conditions) | MatchSet::Or(conditions) =>
			{
				write_context_scope_start::<_, false>(query, context);

				let iter = &mut conditions.iter().filter(|m| *m != &MatchSet::Any);
				if let Some(c) = iter.next()
				{
					Self::write_where_clause(WriteContext::InWhereCondition, ident, c, query);
				}

				let separator = match match_condition
				{
					MatchSet::And(_) => sql::AND,
					MatchSet::Or(_) => sql::OR,
					_ => unreachable!(),
				};

				conditions.iter().for_each(|c| {
					query.push(separator);
					Self::write_where_clause(WriteContext::InWhereCondition, ident, c, query);
				});

				write_context_scope_end(query);
			},

			MatchSet::Contains(match_expense) =>
			{
				const COLUMNS: ExpenseColumns<&'static str> = ExpenseColumns::default();
				let subquery_ident = SnakeCase::from((ident, 2));

				query
					.push(context)
					.push(sql::EXISTS)
					.push('(')
					.push(sql::SELECT)
					.push_from(ExpenseColumns::TABLE_NAME, subquery_ident)
					.push(sql::WHERE)
					.push_equal(
						COLUMNS.scope(subquery_ident).timesheet_id,
						COLUMNS.scope(ident).timesheet_id,
					);

				Self::write_where_clause(
					WriteContext::AcceptingAnotherWhereCondition,
					subquery_ident,
					match_expense,
					query,
				);

				query.push(')');
			},
			MatchSet::Not(condition) => write_negated(query, context, ident, condition.deref()),
		};

		tracing::trace!("After: {}", query.sql());
		WriteContext::AcceptingAnotherWhereCondition
	}
}

impl WriteWhereClause<Postgres, &MatchStr<String>> for PgSchema
{
	#[tracing::instrument(name = "write MatchStr<String> clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchStr<String>,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		// NOTE: we cannot use certain helpers defined above, as some do not
		//       sanitize `match_condition` and are thus susceptible to SQL injection.
		match match_condition
		{
			MatchStr::And(conditions) => write_boolean_group::<_, _, _, _, true>(
				query,
				context,
				ident,
				&mut conditions.iter().filter(|m| MatchStr::Any.eq(*m)),
			),
			MatchStr::Any => write_any(query, context),
			MatchStr::Contains(string) =>
			{
				query
					.separated(' ')
					.push(context)
					.push(ident)
					.push(sql::LIKE)
					.push_bind(PgContains(string).to_string());
			},
			MatchStr::EqualTo(string) =>
			{
				query
					.separated(' ')
					.push(context)
					.push(ident)
					.push_unseparated('=')
					.push_bind(string.clone());
			},
			MatchStr::Not(condition) => write_negated(query, context, ident, condition.deref()),
			MatchStr::Or(conditions) => write_boolean_group::<_, _, _, _, false>(
				query,
				context,
				ident,
				&mut conditions.iter().filter(|m| MatchStr::Any.eq(*m)),
			),
			MatchStr::Regex(regex) =>
			{
				query
					.separated(' ')
					.push(context)
					.push(ident)
					.push_unseparated('~')
					.push_bind(regex.clone());
			},
		};

		tracing::trace!("After: {}", query.sql());
		WriteContext::AcceptingAnotherWhereCondition
	}
}

impl WriteWhereClause<Postgres, &MatchEmployee> for PgSchema
{
	#[tracing::instrument(name = "write MatchEmployee clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchEmployee,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = EmployeeColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(
				write_non_default_condition(
					write_non_default_condition(context, columns.id, &match_condition.id, query),
					columns.name,
					&match_condition.name,
					query,
				),
				columns.status,
				&match_condition.status,
				query,
			),
			columns.title,
			&match_condition.title,
			query,
		)
	}
}

impl WriteWhereClause<Postgres, &MatchExpense> for PgSchema
{
	#[tracing::instrument(name = "write MatchExpense clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchExpense,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = ExpenseColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(
				write_non_default_condition(
					write_non_default_condition(
						write_non_default_condition(
							context,
							columns.id,
							&match_condition.id,
							query,
						),
						columns.category,
						&match_condition.category,
						query,
					),
					// NOTE: `cost` is stored as text on the DB
					columns.typecast("numeric").cost,
					&match_condition.cost.map_ref(|c| c.amount),
					query,
				),
				columns.description,
				&match_condition.description,
				query,
			),
			columns.timesheet_id,
			&match_condition.timesheet_id,
			query,
		)
	}
}

impl WriteWhereClause<Postgres, &MatchInvoice> for PgSchema
{
	#[tracing::instrument(name = "write MatchInvoice clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchInvoice,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = JobColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(
				write_non_default_condition(
					context,
					columns.invoice_date_issued,
					&match_condition.date_issued,
					query,
				),
				columns.invoice_date_paid,
				&match_condition.date_paid,
				query,
			),
			// NOTE: `hourly_rate` is stored as text on the DB
			columns.typecast("numeric").invoice_hourly_rate,
			&match_condition.hourly_rate.map_ref(|r| r.amount),
			query,
		)
	}
}

impl WriteWhereClause<Postgres, &MatchJob> for PgSchema
{
	#[tracing::instrument(name = "write MatchJob clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchJob,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = JobColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(
				write_non_default_condition(
					write_non_default_condition(
						write_non_default_condition(
							write_non_default_condition(
								write_non_default_condition(
									context,
									columns.date_close,
									&match_condition
										.date_close
										.map_ref(|m| m.map_ref(|d| PgTimestampTz(*d))),
									query,
								),
								columns.date_open,
								&match_condition.date_open.map_ref(|d| PgTimestampTz(*d)),
								query,
							),
							columns.id,
							&match_condition.id,
							query,
						),
						columns.increment,
						&match_condition.increment.map_ref(|i| PgInterval(i.into_inner())),
						query,
					),
					ident,
					&match_condition.invoice,
					query,
				),
				columns.notes,
				&match_condition.notes,
				query,
			),
			columns.objectives,
			&match_condition.objectives,
			query,
		)
	}
}

impl WriteWhereClause<Postgres, &MatchOrganization> for PgSchema
{
	#[tracing::instrument(name = "write MatchOrganization clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchOrganization,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = OrganizationColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(context, columns.id, &match_condition.id, query),
			columns.name,
			&match_condition.name,
			query,
		)
	}
}

impl WriteWhereClause<Postgres, &MatchTimesheet> for PgSchema
{
	#[tracing::instrument(name = "write MatchTimesheet clause", level = "trace", skip_all)]
	fn write_where_clause<Ident>(
		context: WriteContext,
		ident: Ident,
		match_condition: &MatchTimesheet,
		query: &mut QueryBuilder<Postgres>,
	) -> WriteContext
	where
		Ident: Copy + Display,
	{
		let columns = TimesheetColumns::default().scope(ident);

		write_non_default_condition(
			write_non_default_condition(
				write_non_default_condition(
					write_non_default_condition(context, columns.id, &match_condition.id, query),
					columns.time_begin,
					&match_condition.time_begin.map_ref(|d| PgTimestampTz(*d)),
					query,
				),
				columns.time_end,
				&match_condition.time_end.map_ref(|m| m.map_ref(|d| PgTimestampTz(*d))),
				query,
			),
			columns.work_notes,
			&match_condition.work_notes,
			query,
		)
	}
}

/// Ensure `condition` is not its [`Default`] value and then `write` it
fn write_non_default_condition<'a, C, T>(
	context: WriteContext,
	column: C,
	condition: &'a T,
	query: &mut QueryBuilder<Postgres>,
) -> WriteContext
where
	C: Copy + Display,
	T: Default + PartialEq,
	PgSchema: WriteWhereClause<Postgres, &'a T>,
{
	match T::default().eq(condition)
	{
		true => context,
		false => PgSchema::write_where_clause(context, column, condition, query),
	}
}
