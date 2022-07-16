use core::time::Duration;

use clinvoice_adapter::schema::JobAdapter;
use clinvoice_finance::{ExchangeRates, Exchangeable};
use clinvoice_schema::{
	chrono::{DateTime, Utc},
	Invoice,
	Job,
	Organization,
};
use sqlx::{Executor, Postgres, Result};

use super::PgJob;
use crate::{fmt::DateTimeExt, schema::util};

#[async_trait::async_trait]
impl JobAdapter for PgJob
{
	async fn create<'c, TConn>(
		connection: TConn,
		client: Organization,
		date_close: Option<DateTime<Utc>>,
		date_open: DateTime<Utc>,
		increment: Duration,
		invoice: Invoice,
		notes: String,
		objectives: String,
	) -> Result<Job>
	where
		TConn: Executor<'c, Database = Postgres>,
	{
		let standardized_rate = ExchangeRates::new()
			.await
			.map(|r| invoice.hourly_rate.exchange(Default::default(), &r))
			.map_err(util::finance_err_to_sqlx)?;

		let row = sqlx::query!(
			"INSERT INTO jobs
				(client_id, date_close, date_open, increment, invoice_date_issued, invoice_date_paid, invoice_hourly_rate, notes, objectives)
			VALUES
				($1,        $2,         $3,        $4,        $5,                  $6,                $7,                  $8,    $9)
			RETURNING id;",
			client.id,
			date_close,
			date_open,
			increment as _,
			invoice.date.as_ref().map(|d| d.issued),
			invoice.date.as_ref().and_then(|d| d.paid),
			standardized_rate.amount.to_string() as _,
			notes,
			objectives,
		)
		.fetch_one(connection)
		.await?;

		Ok(Job {
			client,
			date_close,
			date_open,
			id: row.id,
			increment,
			invoice,
			notes,
			objectives,
		}
		.pg_sanitize())
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use clinvoice_adapter::schema::{LocationAdapter, OrganizationAdapter};
	use clinvoice_finance::{ExchangeRates, Exchangeable};
	use clinvoice_schema::{chrono::Utc, Currency, Invoice, Money};
	use pretty_assertions::assert_eq;

	use super::{JobAdapter, PgJob};
	use crate::schema::{util, PgLocation, PgOrganization};

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, "Earth".into(), None)
			.await
			.unwrap();

		let organization = PgOrganization::create(&connection, earth, "Some Organization".into())
			.await
			.unwrap();

		let job = PgJob::create(
			&connection,
			organization.clone(),
			None,
			Utc::now(),
			Duration::new(7640, 0),
			Invoice {
				date: None,
				hourly_rate: Money::new(13_27, 2, Currency::Usd),
			},
			String::new(),
			"Write the test".into(),
		)
		.await
		.unwrap();

		let row = sqlx::query!(
			"SELECT
					id,
					client_id,
					date_close,
					date_open,
					increment,
					invoice_date_issued,
					invoice_date_paid,
					invoice_hourly_rate,
					notes,
					objectives
				FROM jobs
				WHERE id = $1;",
			job.id,
		)
		.fetch_one(&connection)
		.await
		.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(job.id, row.id);
		assert_eq!(job.client.id, row.client_id);
		assert_eq!(organization.id, row.client_id);
		assert_eq!(job.date_close, row.date_close);
		assert_eq!(job.date_open, row.date_open);
		assert_eq!(job.increment, util::duration_from(row.increment).unwrap());
		assert_eq!(None, row.invoice_date_issued);
		assert_eq!(None, row.invoice_date_paid);
		assert_eq!(
			job.invoice
				.hourly_rate
				.exchange(Default::default(), &ExchangeRates::new().await.unwrap()),
			Money {
				amount: row.invoice_hourly_rate.parse().unwrap(),
				..Default::default()
			},
		);
		assert_eq!(job.notes, row.notes);
		assert_eq!(job.objectives, row.objectives);
	}
}
