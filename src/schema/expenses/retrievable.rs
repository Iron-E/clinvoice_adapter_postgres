use clinvoice_adapter::{
	fmt::{sql, QueryBuilderExt, TableToSql},
	schema::columns::ExpenseColumns,
	Retrievable,
	WriteWhereClause,
};
use clinvoice_match::MatchExpense;
use clinvoice_schema::Expense;
use futures::{future, TryFutureExt, TryStreamExt};
use money2::{ExchangeRates, Exchangeable};
use sqlx::{Pool, Postgres, QueryBuilder, Result};

use super::PgExpenses;
use crate::{schema::util, PgSchema};

/// Implementors of this trait are capable of being retrieved from a [`Database`].
#[async_trait::async_trait]
impl Retrievable for PgExpenses
{
	/// The [`Database`] where data of type [`Updatable::Entity`] is being stored.
	type Db = Postgres;
	/// The type of data that is to be [`update`](Deletable::update)d.
	type Entity = Expense;
	/// The type used for [match](clinvoice_match)ing.
	type Match = MatchExpense;

	/// Retrieve all [`Expense`]s (via `connection`) that match the `match_condition`.
	async fn retrieve(
		connection: &Pool<Postgres>,
		match_condition: &Self::Match,
	) -> Result<Vec<Self::Entity>>
	{
		const COLUMNS: ExpenseColumns<&str> = ExpenseColumns::default();

		let columns = COLUMNS.default_scope();
		let exchange_rates_fut = ExchangeRates::new().map_err(util::finance_err_to_sqlx);
		let mut query = QueryBuilder::new(sql::SELECT);

		query
			.push_columns(&columns)
			.push_default_from::<ExpenseColumns<char>>();

		let exchange_rates = exchange_rates_fut.await?;
		PgSchema::write_where_clause(
			Default::default(),
			ExpenseColumns::<char>::DEFAULT_ALIAS,
			&match_condition.exchange_ref(Default::default(), &exchange_rates),
			&mut query,
		);

		query
			.prepare()
			.fetch(connection)
			.and_then(|row| future::ready(Self::row_to_view(COLUMNS, &row)))
			.try_collect()
			.await
	}
}
