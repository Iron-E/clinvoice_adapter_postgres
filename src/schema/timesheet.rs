mod deletable;
mod retrievable;
mod timesheet_adapter;
mod updatable;

use winvoice_adapter::schema::columns::{
	EmployeeColumns,
	JobColumns,
	OrganizationColumns,
	TimesheetColumns,
};
use winvoice_schema::{Expense, Timesheet};
use money2::{Decimal, Money};
use sqlx::{error::UnexpectedNullError, postgres::PgRow, Error, Executor, Postgres, Result, Row};

use super::{util, PgEmployee, PgJob};

/// Implementor of the [`TimesheetAdapter`](winvoice_adapter::schema::TimesheetAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgTimesheet;

impl PgTimesheet
{
	pub(super) async fn row_to_view<
		'connection,
		Conn,
		TimesheetColumnNames,
		EmployeeColumnNames,
		ExpenseColumnNames,
		JobColumnNames,
		OrganizationColumnNames,
	>(
		connection: Conn,
		columns: TimesheetColumns<TimesheetColumnNames>,
		employee_columns: EmployeeColumns<EmployeeColumnNames>,
		expenses_ident: ExpenseColumnNames,
		job_columns: JobColumns<JobColumnNames>,
		organization_columns: OrganizationColumns<OrganizationColumnNames>,
		row: &PgRow,
	) -> Result<Timesheet>
	where
		Conn: Executor<'connection, Database = Postgres>,
		EmployeeColumnNames: AsRef<str>,
		JobColumnNames: AsRef<str>,
		OrganizationColumnNames: AsRef<str>,
		TimesheetColumnNames: AsRef<str>,
		ExpenseColumnNames: AsRef<str>,
	{
		let job_fut = PgJob::row_to_view(connection, job_columns, organization_columns, row);
		Ok(Timesheet {
			employee: PgEmployee::row_to_view(employee_columns, row),
			id: row.try_get(columns.id.as_ref())?,
			time_begin: row.try_get(columns.time_begin.as_ref())?,
			time_end: row.try_get(columns.time_end.as_ref())?,
			work_notes: row.try_get(columns.work_notes.as_ref())?,
			expenses: row
				.try_get(expenses_ident.as_ref())
				.and_then(|raw_expenses: Vec<(_, String, _, _, _)>| {
					raw_expenses
						.into_iter()
						.map(|(category, cost, description, id, timesheet_id)| {
							Ok(Expense {
								category,
								description,
								id,
								timesheet_id,
								cost: Money {
									amount: cost
										.parse::<Decimal>()
										.map_err(|e| util::finance_err_to_sqlx(e.into()))?,
									..Default::default()
								},
							})
						})
						.collect::<Result<Vec<_>>>()
				})
				.or_else(|e| match e
				{
					Error::ColumnDecode { source: s, .. } if s.is::<UnexpectedNullError>() =>
					{
						Ok(Vec::new())
					},
					_ => Err(e),
				})?,
			job: job_fut.await?,
		})
	}
}
