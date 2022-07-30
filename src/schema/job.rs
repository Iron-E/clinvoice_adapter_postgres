mod deletable;
mod job_adapter;
mod retrievable;
mod updatable;

use clinvoice_adapter::schema::columns::{JobColumns, OrganizationColumns};
use clinvoice_schema::{Invoice, InvoiceDate, Job};
use money2::{Decimal, Money};
use sqlx::{postgres::PgRow, Executor, Postgres, Result, Row};

use super::{util, PgOrganization};

/// Implementor of the [`JobAdapter`](clinvoice_adapter::schema::JobAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgJob;

impl PgJob
{
	pub(super) async fn row_to_view<'connection, Conn, JobColumnName, OrgColumnName>(
		connection: Conn,
		columns: JobColumns<JobColumnName>,
		organization_columns: OrganizationColumns<OrgColumnName>,
		row: &PgRow,
	) -> Result<Job>
	where
		Conn: Executor<'connection, Database = Postgres>,
		JobColumnName: AsRef<str>,
		OrgColumnName: AsRef<str>,
	{
		let client_fut = PgOrganization::row_to_view(connection, organization_columns, row);

		let amount = row
			.try_get::<String, _>(columns.invoice_hourly_rate.as_ref())
			.and_then(|raw_hourly_rate| {
				raw_hourly_rate
					.parse::<Decimal>()
					.map_err(|e| util::finance_err_to_sqlx(e.into()))
			})?;

		let increment = row
			.try_get(columns.increment.as_ref())
			.and_then(util::duration_from)?;

		let invoice_date_paid = row.try_get::<Option<_>, _>(columns.invoice_date_paid.as_ref())?;
		Ok(Job {
			date_close: row.try_get(columns.date_close.as_ref())?,
			date_open: row.try_get(columns.date_open.as_ref())?,
			id: row.try_get(columns.id.as_ref())?,
			increment,
			invoice: Invoice {
				date: row
					.try_get::<Option<_>, _>(columns.invoice_date_issued.as_ref())
					.map(|date| {
						date.map(|d| InvoiceDate {
							issued: d,
							paid: invoice_date_paid,
						})
					})?,
				hourly_rate: Money {
					amount,
					..Default::default()
				},
			},
			notes: row.try_get(columns.notes.as_ref())?,
			objectives: row.try_get(columns.objectives.as_ref())?,
			client: client_fut.await?,
		})
	}
}
