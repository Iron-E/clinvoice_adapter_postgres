use core::time::Duration;
use std::collections::BTreeSet;

use money2::{Exchange, ExchangeRates};
use sqlx::{Postgres, Result, Transaction};
use winvoice_adapter::schema::JobAdapter;
use winvoice_schema::{
	chrono::{DateTime, Utc},
	Department,
	Id,
	Invoice,
	Job,
	Organization,
};

use super::PgJob;
use crate::{fmt::DateTimeExt, schema::util};

#[async_trait::async_trait]
impl JobAdapter for PgJob
{
	async fn create(
		connection: &mut Transaction<Postgres>,
		client: Organization,
		date_close: Option<DateTime<Utc>>,
		date_open: DateTime<Utc>,
		departments: BTreeSet<Department>,
		increment: Duration,
		invoice: Invoice,
		notes: String,
		objectives: String,
	) -> Result<Job>
	{
		let standardized_rate = ExchangeRates::new()
			.await
			.map(|r| invoice.hourly_rate.exchange(Default::default(), &r))
			.map_err(util::finance_err_to_sqlx)?;

		let id = Id::new_v4();
		sqlx::query!(
			"INSERT INTO jobs
				(id, client_id, date_close, date_open, increment, invoice_date_issued, invoice_date_paid, invoice_hourly_rate, notes, objectives)
			VALUES
				($1, $2,        $3,         $4,        $5,        $6,                  $7,                $8,                  $9,    $10);",
			id,
			client.id,
			date_close.map(|d| d.naive_utc()),
			date_open.naive_utc(),
			increment as _,
			invoice.date.as_ref().map(|d| d.issued.naive_utc()),
			invoice.date.as_ref().and_then(|d| d.paid.map(|p| p.naive_utc())),
			standardized_rate.amount.to_string() as _,
			notes,
			objectives,
		)
		.execute(&mut *connection)
		.await?;

		util::insert_into_job_departments(connection, &departments, id).await?;

		Ok(Job {
			client,
			date_close,
			date_open,
			departments,
			id,
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

	use mockd::{address, company, job, words};
	use money2::{Exchange, ExchangeRates};
	use pretty_assertions::assert_eq;
	use winvoice_adapter::schema::{DepartmentAdapter, LocationAdapter, OrganizationAdapter};
	use winvoice_schema::{chrono::Utc, Currency, Department, Id, Invoice, Money};

	use super::{JobAdapter, PgJob};
	use crate::schema::{util, PgDepartment, PgLocation, PgOrganization};

	#[tokio::test]
	async fn create()
	{
		let connection = util::connect().await;

		let (department, location) = futures::try_join!(
			PgDepartment::create(&connection, job::level()),
			PgLocation::create(&connection, None, address::country(), None),
		)
		.unwrap();

		let mut tx = connection.begin().await.unwrap();
		let organization =
			PgOrganization::create(&connection, location, company::company()).await.unwrap();

		let job = PgJob::create(
			&mut tx,
			organization.clone(),
			None,
			Utc::now(),
			[department].into_iter().collect(),
			Duration::new(7640, 0),
			Invoice { date: None, hourly_rate: Money::new(13_27, 2, Currency::Usd) },
			String::new(),
			words::sentence(5),
		)
		.await
		.unwrap();

		tx.commit().await.unwrap();
		let row = sqlx::query!(
			r#"SELECT
					J.id,
					J.client_id,
					J.date_close,
					J.date_open,
					array_agg(D) as "departments!: Vec<(Id, String)>",
					J.increment,
					J.invoice_date_issued,
					J.invoice_date_paid,
					J.invoice_hourly_rate,
					J.notes,
					J.objectives
				FROM jobs J
				LEFT JOIN job_departments Jd ON Jd.job_id = J.id
				LEFT JOIN departments D ON D.id = Jd.department_id
				WHERE J.id = $1
				GROUP BY J.id"#,
			job.id,
		)
		.fetch_one(&connection)
		.await
		.unwrap();

		// Assert ::create writes accurately to the DB
		assert_eq!(job.id, row.id);
		assert_eq!(job.client.id, row.client_id);
		assert_eq!(organization.id, row.client_id);
		assert_eq!(
			job.departments,
			row.departments.into_iter().map(|(id, name)| Department { id, name }).collect()
		);
		assert_eq!(job.date_close, row.date_close.map(util::naive_date_to_utc));
		assert_eq!(job.date_open, row.date_open.and_utc());
		assert_eq!(job.increment, util::duration_from(row.increment).unwrap());
		assert_eq!(None, row.invoice_date_issued);
		assert_eq!(None, row.invoice_date_paid);
		assert_eq!(
			job.invoice
				.hourly_rate
				.exchange(Default::default(), &ExchangeRates::new().await.unwrap()),
			Money { amount: row.invoice_hourly_rate.parse().unwrap(), ..Default::default() },
		);
		assert_eq!(job.notes, row.notes);
		assert_eq!(job.objectives, row.objectives);
	}
}
