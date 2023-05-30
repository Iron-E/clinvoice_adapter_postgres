use winvoice_adapter::schema::Adapter;

use super::{
	PgContact,
	PgEmployee,
	PgExpenses,
	PgJob,
	PgLocation,
	PgOrganization,
	PgSchema,
	PgTimesheet,
};

impl Adapter for PgSchema
{
	type Contact = PgContact;
	type Employee = PgEmployee;
	type Expenses = PgExpenses;
	type Job = PgJob;
	type Location = PgLocation;
	type Organization = PgOrganization;
	type Timesheet = PgTimesheet;
}
