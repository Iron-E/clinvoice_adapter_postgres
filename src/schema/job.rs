mod deletable;
mod job_adapter;
mod retrievable;
mod updatable;

use money2::{Decimal, Money};
use sqlx::{error::UnexpectedNullError, postgres::PgRow, Error, Executor, Postgres, Result, Row};
use winvoice_adapter::schema::columns::{JobColumns, OrganizationColumns};
use winvoice_schema::{chrono::NaiveDateTime, Department, Invoice, InvoiceDate, Job};

use super::{util, PgOrganization};

/// Implementor of the [`JobAdapter`](winvoice_adapter::schema::JobAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgJob;

impl PgJob
{
	/// Convert the `row` into a typed [`Job`].
	pub async fn row_to_view<'connection, Conn, JobColumnT, DepartmentColumnT, OrgColumnT>(
		connection: Conn,
		columns: JobColumns<JobColumnT>,
		departments_ident: DepartmentColumnT,
		organization_columns: OrganizationColumns<OrgColumnT>,
		row: &PgRow,
	) -> Result<Job>
	where
		Conn: Executor<'connection, Database = Postgres>,
		JobColumnT: AsRef<str>,
		DepartmentColumnT: AsRef<str>,
		OrgColumnT: AsRef<str>,
	{
		let client_fut = PgOrganization::row_to_view(connection, organization_columns, row);

		let amount = row.try_get::<String, _>(columns.invoice_hourly_rate.as_ref()).and_then(|raw_hourly_rate| {
			raw_hourly_rate.parse::<Decimal>().map_err(|e| util::finance_err_to_sqlx(e.into()))
		})?;

		let increment = row.try_get(columns.increment.as_ref()).and_then(util::duration_from)?;

		let invoice_date_paid = row.try_get(columns.invoice_date_paid.as_ref()).map(util::naive_date_opt_to_utc)?;

		Ok(Job {
			date_close: row.try_get(columns.date_close.as_ref()).map(util::naive_date_opt_to_utc)?,
			date_open: row.try_get(columns.date_open.as_ref()).map(util::naive_date_to_utc)?,
			departments: row
				.try_get(departments_ident.as_ref())
				.map(|raw_departments: Vec<_>| {
					raw_departments.into_iter().map(|(id, name)| Department { id, name }).collect()
				})
				.or_else(|e| match e
				{
					Error::ColumnDecode { source: s, .. } if s.is::<UnexpectedNullError>() => Ok(Default::default()),
					_ => Err(e),
				})?,
			id: row.try_get(columns.id.as_ref())?,
			increment,
			invoice: Invoice {
				date: row.try_get(columns.invoice_date_issued.as_ref()).map(|date: Option<NaiveDateTime>| {
					date.map(|d| InvoiceDate { issued: d.and_utc(), paid: invoice_date_paid })
				})?,
				hourly_rate: Money { amount, ..Default::default() },
			},
			notes: row.try_get(columns.notes.as_ref())?,
			objectives: row.try_get(columns.objectives.as_ref())?,
			client: client_fut.await?,
		})
	}
}
