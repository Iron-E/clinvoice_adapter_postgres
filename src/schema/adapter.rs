use sqlx::Postgres;
use winvoice_adapter::schema::Adapter;
use super::{PgSchema, PgContact, PgEmployee, PgTimesheet, PgOrganization, PgLocation, PgJob, PgExpenses};

impl Adapter for PgSchema {
    type Db = Postgres;
    type Contact = PgContact;
    type Employee = PgEmployee;
    type Expenses = PgExpenses;
    type Job = PgJob;
    type Location = PgLocation;
    type Organization = PgOrganization;
    type Timesheet = PgTimesheet;
}
