mod deletable;
mod retrievable;
mod timesheet_adapter;
mod updatable;

use money2::{Decimal, Money};
use sqlx::{error::UnexpectedNullError, postgres::PgRow, Error, Executor, Postgres, Result, Row};
use winvoice_adapter::schema::columns::{
	DepartmentColumns,
	EmployeeColumns,
	JobColumns,
	OrganizationColumns,
	TimesheetColumns,
};
use winvoice_schema::{Expense, Timesheet};

use super::{util, PgEmployee, PgJob};

/// Implementor of the [`TimesheetAdapter`](winvoice_adapter::schema::TimesheetAdapter) for the
/// [`Postgres`](sqlx::Postgres) database.
pub struct PgTimesheet;

impl PgTimesheet
{
	/// Convert the `row` into a typed [`Timesheet`].
	pub async fn row_to_view<
		'connection,
		Conn,
		TimesheetColumnT,
		DepartmentColumnT,
		EmployeeColumnT,
		ExpenseColumnT,
		JobColumnT,
		OrganizationColumnT,
	>(
		connection: Conn,
		columns: TimesheetColumns<TimesheetColumnT>,
		departments_ident: DepartmentColumnT,
		department_columns: DepartmentColumns<DepartmentColumnT>,
		employee_columns: EmployeeColumns<EmployeeColumnT>,
		expenses_ident: ExpenseColumnT,
		job_columns: JobColumns<JobColumnT>,
		organization_columns: OrganizationColumns<OrganizationColumnT>,
		row: &PgRow,
	) -> Result<Timesheet>
	where
		Conn: Executor<'connection, Database = Postgres>,
		DepartmentColumnT: AsRef<str>,
		EmployeeColumnT: AsRef<str>,
		ExpenseColumnT: AsRef<str>,
		JobColumnT: AsRef<str>,
		OrganizationColumnT: AsRef<str>,
		TimesheetColumnT: AsRef<str>,
	{
		let job_fut = PgJob::row_to_view(
			connection,
			job_columns,
			departments_ident,
			organization_columns,
			row,
		);
		Ok(Timesheet {
			employee: PgEmployee::row_to_view(employee_columns, department_columns, row),
			id: row.try_get(columns.id.as_ref())?,
			time_begin: row.try_get(columns.time_begin.as_ref()).map(util::naive_date_to_utc)?,
			time_end: row.try_get(columns.time_end.as_ref()).map(util::naive_date_opt_to_utc)?,
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
						Ok(Default::default())
					},
					_ => Err(e),
				})?,
			job: job_fut.await?,
		})
	}
}
