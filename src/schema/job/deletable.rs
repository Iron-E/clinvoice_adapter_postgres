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

	async fn delete<'entity, Conn, Iter>(connection: &Conn, entities: Iter) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Iterator<Item = &'entity Self::Entity> + Send,
		for<'con> &'con Conn: Executor<'con, Database = Self::Db>,
	{
		fn mapper(j: &Job) -> PgUuid
		{
			PgUuid::from(j.id)
		}

		// TODO: use `for<'a> |e: &'a Job| e.id`
		PgSchema::delete::<_, _, JobColumns>(connection, entities.map(mapper)).await
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use mockd::{address, company, words};
	use money2::{Currency, Exchange, HistoricalExchangeRates, Money};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{DepartmentAdapter, JobAdapter, LocationAdapter, OrganizationAdapter},
		Deletable,
		Retrievable,
	};
	use winvoice_match::Match;
	use winvoice_schema::{
		chrono::{TimeZone, Utc},
		Invoice,
		InvoiceDate,
	};

	use crate::schema::{util, PgDepartment, PgJob, PgLocation, PgOrganization};

	#[tokio::test]
	async fn delete()
	{
		let connection = util::connect();

		let (department, location) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let organization = PgOrganization::create(&mut tx, location.clone(), company::company()).await.unwrap();

		let job = PgJob::create(
			&mut tx,
			organization.clone(),
			Utc.with_ymd_and_hms(1990, 08, 01, 09, 00, 00).latest(),
			Utc.with_ymd_and_hms(1990, 07, 12, 14, 10, 00).unwrap(),
			[department.clone()].into_iter().collect(),
			Duration::from_secs(300),
			Invoice { date: None, hourly_rate: Money::new(20_00, 2, Currency::Usd) },
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let job2 = PgJob::create(
			&mut tx,
			organization.clone(),
			Utc.with_ymd_and_hms(3000, 01, 16, 10, 00, 00).latest(),
			Utc.with_ymd_and_hms(3000, 01, 12, 09, 15, 42).unwrap(),
			[department.clone()].into_iter().collect(),
			Duration::from_secs(900),
			Invoice {
				date: InvoiceDate { issued: Utc.with_ymd_and_hms(3000, 01, 17, 12, 30, 00).unwrap(), paid: None }
					.into(),
				hourly_rate: Money::new(299_99, 2, Currency::Jpy),
			},
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		let job3 = PgJob::create(
			&mut tx,
			organization.clone(),
			Utc.with_ymd_and_hms(2011, 03, 17, 13, 07, 07).latest(),
			Utc.with_ymd_and_hms(2011, 03, 17, 12, 07, 07).unwrap(),
			[department.clone()].into_iter().collect(),
			Duration::from_secs(900),
			Invoice {
				date: InvoiceDate {
					issued: Utc.with_ymd_and_hms(2011, 03, 18, 08, 00, 00).unwrap(),
					paid: Utc.with_ymd_and_hms(2011, 03, 19, 17, 00, 00).latest(),
				}
				.into(),
				hourly_rate: Money::new(20_00, 2, Currency::Eur),
			},
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();

		assert!(PgOrganization::delete(&connection, [&organization].into_iter()).await.is_err());
		PgJob::delete(&connection, [&job, &job2].into_iter()).await.unwrap();

		let exchange_rates = HistoricalExchangeRates::try_index(None).await.unwrap();
		assert_eq!(
			PgJob::retrieve(&connection, (Match::from(job.id) | job2.id.into() | job3.id.into()).into())
				.await
				.unwrap()
				.as_slice(),
			&[job3.exchange(Default::default(), &exchange_rates)],
		);
	}
}
