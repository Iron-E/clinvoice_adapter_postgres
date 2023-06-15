use sqlx::{Executor, Postgres, Result};
use winvoice_adapter::{schema::columns::JobColumns, Deletable};
use winvoice_schema::Job;

use super::PgJob;
use crate::{fmt::PgUuid, PgSchema};

#[async_trait::async_trait]
impl Deletable for PgJob
{
	type Db = Postgres;
	type Entity = Job;

	async fn delete<'connection, 'entity, Conn, Iter>(
		connection: Conn,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Conn: Executor<'connection, Database = Self::Db>,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
	{
		const fn mapper(j: &Job) -> PgUuid
		{
			PgUuid(j.id)
		}

		// TODO: use `for<'a> |e: &'a Job| e.id`
		PgSchema::delete::<_, _, JobColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use money2::{Currency, Exchange, ExchangeRates, Money};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{JobAdapter, LocationAdapter, OrganizationAdapter},
		Deletable,
		Retrievable,
	};
	use winvoice_match::Match;
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Invoice,
		InvoiceDate,
	};

	use crate::schema::{util, PgJob, PgLocation, PgOrganization};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect().await;

		let earth = PgLocation::create(&connection, None, "Earth".into(), None).await.unwrap();

		let organization =
			PgOrganization::create(&connection, earth.clone(), "Some Organization".into())
				.await
				.unwrap();

		let (job, job2, job3) = futures::try_join!(
			PgJob::create(
				&connection,
				organization.clone(),
				Utc.with_ymd_and_hms(1990, 08, 01, 09, 00, 00).latest(),
				Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
				Duration::from_secs(300),
				Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization.clone(),
				Utc.with_ymd_and_hms(3000, 01, 16, 10, 00, 00).latest(),
				Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
				Duration::from_secs(900),
				Invoice {
					date: Some(InvoiceDate {
						issued: Utc.with_ymd_and_hms(3000, 01, 17, 12, 30, 00).unwrap(),
						paid: None,
					}),
					hourly_rate: Money::new(299_99, 2, Currency::Jpy),
				},
				String::new(),
				"Do something".into()
			),
			PgJob::create(
				&connection,
				organization.clone(),
				Utc.with_ymd_and_hms(2011, 03, 17, 13, 07, 07).latest(),
				Utc.with_ymd_and_hms(2011, 03, 17, 12, 07, 07).unwrap(),
				Duration::from_secs(900),
				Invoice {
					date: Some(InvoiceDate {
						issued: Utc.with_ymd_and_hms(2011, 03, 18, 08, 00, 00).unwrap(),
						paid: Utc.with_ymd_and_hms(2011, 03, 19, 17, 00, 00).latest(),
					}),
					hourly_rate: Money::new(20_00, 2, Currency::Eur),
				},
				String::new(),
				"Do something".into()
			),
		)
		.unwrap();

		assert!(PgOrganization::delete(&connection, [&organization].into_iter()).await.is_err());
		PgJob::delete(&connection, [&job, &job2].into_iter()).await.unwrap();

		let exchange_rates = ExchangeRates::new().await.unwrap();
		assert_eq!(
			PgJob::retrieve(
				&connection,
				Match::Or(vec![job.id.into(), job2.id.into(), job3.id.into()]).into(),
			)
			.await
			.unwrap()
			.as_slice(),
			&[job3.exchange(Default::default(), &exchange_rates)],
		);
	}
}
