//! Contains utilities which help with implementing/testing the
//! [`winvoice_adapter_postgres::schema`].

use core::time::Duration;
use std::{collections::BTreeSet, io};

use money2::Error as FinanceError;
use sqlx::{postgres::types::PgInterval, Error, Executor, Postgres, QueryBuilder, Result};
use winvoice_adapter::fmt::QueryBuilderExt;
use winvoice_schema::{
	chrono::{DateTime, NaiveDateTime, Utc},
	Department,
	Id,
};
#[cfg(test)]
use {
	mockd::{address, job},
	sqlx::PgPool,
	std::sync::OnceLock,
};

#[cfg(test)]
pub(super) async fn connect() -> PgPool
{
	static URL: OnceLock<String> = OnceLock::new();
	PgPool::connect_lazy(&URL.get_or_init(|| dotenvy::var("DATABASE_URL").unwrap())).unwrap()
}

/// Convert a [`PgInterval`] to a concrete [`Duration`]
pub fn duration_from(interval: PgInterval) -> Result<Duration>
{
	const MICROSECONDS_IN_SECOND: u64 = 1000000;
	const NANOSECONDS_IN_MICROSECOND: u32 = 1000;
	const SECONDS_IN_DAY: u64 = 86400;

	if interval.months > 0
	{
		return Err(Error::Decode(
			"`PgInterval` could not be decoded into `Duration` because of nonstandard time \
			 measurement `months`"
				.into(),
		));
	}

	// Ignore negative microseconds
	let microseconds: u64 = interval.microseconds.try_into().unwrap_or(0);

	let seconds = microseconds / MICROSECONDS_IN_SECOND;
	let nanoseconds = NANOSECONDS_IN_MICROSECOND *
		u32::try_from(microseconds % MICROSECONDS_IN_SECOND)
			.expect("`u64::MAX % 1000000` should fit into `u32`");

	Ok(Duration::new(
		seconds +
			u64::try_from(interval.days)
				.map(|days| days * SECONDS_IN_DAY)
				// Ignore negative days
				.unwrap_or(0),
		nanoseconds,
	))
}

pub(crate) async fn insert_into_job_departments<'conn, Conn>(
	connection: Conn,
	departments: &BTreeSet<Department>,
	job_id: Id,
) -> Result<()>
where
	Conn: Executor<'conn, Database = Postgres>,
{
	if !departments.is_empty()
	{
		let mut query = QueryBuilder::new("INSERT INTO job_departments (department_id, job_id) ");
		query.push_values(departments.iter(), |mut q, d| {
			q.push_bind(d.id).push_bind(job_id);
		});

		tracing::debug!("Generated SQL: {}", query.sql());
		query.prepare().execute(connection).await?;
	}

	Ok(())
}

/// Converts a [`NaiveDateTime`] to a [`DateTime<Utc>`].
pub fn naive_date_opt_to_utc(date: Option<NaiveDateTime>) -> Option<DateTime<Utc>>
{
	date.map(naive_date_to_utc)
}

/// Converts a [`NaiveDateTime`] to a [`DateTime<Utc>`].
pub fn naive_date_to_utc(date: NaiveDateTime) -> DateTime<Utc>
{
	date.and_utc()
}

/// Map some [error](money2::Error) `e` to an [`Error`].
pub(super) fn finance_err_to_sqlx(e: FinanceError) -> Error
{
	match e
	{
		FinanceError::Decimal(e2) => Error::Decode(e2.into()),
		FinanceError::Decode { .. } => Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)),
		FinanceError::Io(e2) => Error::Io(e2),
		FinanceError::Reqwest(e2) => Error::Io(io::Error::new(io::ErrorKind::Other, e2)),
		FinanceError::UnsupportedCurrency(_) => Error::Decode(e.into()),
		FinanceError::Zip(e2) => Error::Io(io::Error::new(io::ErrorKind::InvalidData, e2)),
	}
}

#[cfg(test)]
pub fn rand_department_name() -> String
{
	format!("{}{}", job::level(), rand::random::<u16>())
}

#[cfg(test)]
pub fn rand_street_name() -> String
{
	format!("{} {} {}", address::street_prefix(), address::street_name(), address::street_suffix())
}

#[cfg(test)]
mod tests
{
	use pretty_assertions::assert_eq;

	use super::{Duration, PgInterval};

	#[test]
	fn duration_from_interval()
	{
		let test = PgInterval { months: 3, days: 0, microseconds: 0 };

		// Ensure that irregular "months" interval cannot be decoded
		assert!(super::duration_from(test).is_err());

		let test = PgInterval { months: 0, days: 17, microseconds: 7076700 };

		assert_eq!(super::duration_from(test).unwrap(), Duration::new(1468807, 76700000));
	}
}
