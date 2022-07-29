use clinvoice_adapter::{schema::columns::ExpenseColumns, Updatable};
use clinvoice_schema::Expense;
use sqlx::{Postgres, Result, Transaction};

use super::PgExpenses;
use crate::PgSchema;

#[async_trait::async_trait]
impl Updatable for PgExpenses
{
	type Db = Postgres;
	type Entity = Expense;

	async fn update<'entity, TIter>(
		connection: &mut Transaction<Self::Db>,
		entities: TIter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		TIter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		let mut peekable_entities = entities.peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		PgSchema::update(connection, ExpenseColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(&e.category)
					.push_bind(e.cost.amount.to_string())
					.push_bind(&e.description)
					.push_bind(e.id)
					.push_bind(e.timesheet_id);
			});
		})
		.await
	}
}
