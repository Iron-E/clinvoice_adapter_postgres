use futures::{stream, TryFutureExt, TryStreamExt};
use money2::{Exchange, ExchangeRates};
use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::{schema::columns::JobColumns, Updatable};
use winvoice_schema::{
	chrono::{DateTime, Utc},
	Job,
};

use super::PgJob;
use crate::{
	fmt::DateTimeExt,
	schema::{util, PgOrganization},
	PgSchema,
};

#[async_trait::async_trait]
impl Updatable for PgJob
{
	type Db = Postgres;
	type Entity = Job;

	async fn update<'entity, Iter>(
		connection: &mut Transaction<Self::Db>,
		entities: Iter,
	) -> Result<()>
	where
		Self::Entity: 'entity,
		Iter: Clone + Iterator<Item = &'entity Self::Entity> + Send,
	{
		let mut peekable_entities = entities.clone().peekable();

		// There is nothing to do.
		if peekable_entities.peek().is_none()
		{
			return Ok(());
		}

		let exchange_rates = ExchangeRates::new().map_err(util::finance_err_to_sqlx).await?;
		PgSchema::update(connection, JobColumns::default(), |query| {
			query.push_values(peekable_entities, |mut q, e| {
				q.push_bind(e.client.id)
					.push_bind(e.date_close.pg_sanitize())
					.push_bind(e.date_open.pg_sanitize())
					.push_bind(e.id)
					.push_bind(e.increment);

				match e.invoice.date.pg_sanitize()
				{
					Some(ref date) => q.push_bind(date.issued).push_bind(date.paid),
					None => q.push_bind(None::<DateTime<Utc>>).push_bind(None::<DateTime<Utc>>),
				};

				q.push_bind(
					e.invoice
						.hourly_rate
						.exchange(Default::default(), &exchange_rates)
						.amount
						.to_string(),
				)
				.push_bind(&e.notes)
				.push_bind(&e.objectives);
			});
		})
		.await?;

		let clients = entities.clone().map(|e| &e.client);
		PgOrganization::update(connection, clients).await?;

		stream::iter(entities.map(Result::Ok))
			.try_fold(connection, |c, e| async move {
				sqlx::query!("DELETE FROM job_departments WHERE job_id = $1", e.id)
					.execute(&mut *c)
					.await?;

				util::insert_into_job_departments(&mut *c, &e.departments, e.id).await?;
				Ok(c)
			})
			.await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests
{
	use core::time::Duration;

	use mockd::{address, company, words};
	use money2::Money;
	use pretty_assertions::assert_eq;
	use winvoice_adapter::{
		schema::{DepartmentAdapter, JobAdapter, LocationAdapter, OrganizationAdapter},
		Retrievable,
		Updatable,
	};
	use winvoice_schema::{chrono, Invoice, InvoiceDate};

	use crate::{
		fmt::DateTimeExt,
		schema::{util, PgDepartment, PgJob, PgLocation, PgOrganization},
	};

	#[tokio::test]
	async fn update()
	{
		let connection = util::connect().await;

		let (location, location2) = futures::try_join!(
			PgLocation::create(&connection, None, address::country(), None),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let (department, department2) = futures::try_join!(
			PgDepartment::create(&connection, util::rand_department_name()),
			PgDepartment::create(&connection, util::rand_department_name()),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let organization =
			PgOrganization::create(&mut tx, location, company::company()).await.unwrap();

		let mut job = PgJob::create(
			&mut tx,
			organization,
			None,
			chrono::Utc::now(),
			[department].into_iter().collect(),
			Duration::from_secs(900),
			Default::default(),
			words::sentence(5),
			words::sentence(5),
		)
		.await
		.unwrap();

		job.client.location = location2;
		job.departments = [department2].into_iter().collect();
		job.client.name = util::different_string(&job.client.name);
		job.date_close = chrono::Utc::now().into();
		job.increment = Duration::from_secs(300);
		job.invoice = Invoice {
			date: InvoiceDate {
				issued: chrono::Utc::now(),
				paid: Some(chrono::Utc::now() + chrono::Duration::seconds(300)),
			}
			.into(),
			hourly_rate: Money::new(200_00, 2, Default::default()),
		};
		job.notes = util::different_string(&job.notes);
		job.objectives = util::different_string(&job.notes);

		PgJob::update(&mut tx, [&job].into_iter()).await.unwrap();
		tx.commit().await.unwrap();

		let db_job = PgJob::retrieve(&connection, job.id.into()).await.unwrap().pop().unwrap();

		assert_eq!(job.client, db_job.client);
		assert_eq!(job.date_close.pg_sanitize(), db_job.date_close);
		assert_eq!(job.date_open.pg_sanitize(), db_job.date_open);
		assert_eq!(job.departments, db_job.departments);
		assert_eq!(job.id, db_job.id);
		assert_eq!(job.increment, db_job.increment);
		assert_eq!(job.invoice.pg_sanitize(), db_job.invoice);
		assert_eq!(job.notes, db_job.notes);
		assert_eq!(job.objectives, db_job.objectives);
	}
}
