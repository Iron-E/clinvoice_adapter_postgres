use sqlx::{Acquire, Executor, Postgres, Result};
use winvoice_adapter::Initializable;

use super::PgSchema;

/// Initialize the `locations` table.
async fn init_locations<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS locations
		(
			currency text,
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			outer_id bigint REFERENCES locations(id),
			name text NOT NULL,

			CONSTRAINT locations__not_outside_self CHECK (id <> outer_id)
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize `organizations` table.
async fn init_organizations<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS organizations
		(
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			location_id bigint NOT NULL REFERENCES locations(id),
			name text NOT NULL
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `employees` table.
async fn init_employees<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS employees
		(
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			name text NOT NULL,
			status text NOT NULL,
			title text NOT NULL
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `contact_information` table.
async fn init_contact_info<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		r#"CREATE TABLE IF NOT EXISTS contact_information
		(
			label text NOT NULL PRIMARY KEY,

			address_id bigint REFERENCES locations(id),
			email text CHECK (email ~ '^.*@.*\..*$'),
			other text,
			phone text CHECK (phone ~ '^[0-9\- ]+$'),

			CONSTRAINT contact_information__is_variant CHECK
			(
				( -- ContactKind::Address
					address_id IS NOT null AND
					email IS null AND
					other IS null AND
					phone IS null
				)
				OR
				( -- ContactKind::Email
					address_id IS null AND
					email IS NOT null AND
					other IS null AND
					phone IS null
				)
				OR
				( -- ContactKind::Other
					address_id IS null AND
					email IS null AND
					other IS NOT null AND
					phone IS null
				)
				OR
				( -- ContactKind::Phone
					address_id IS null AND
					email IS null AND
					other IS null AND
					phone IS NOT null
				)
			)
		);"#
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `money_in_eur` type.
async fn init_money<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		r"DO $$
BEGIN
	IF NOT EXISTS (SELECT FROM pg_type WHERE typname = 'money_in_eur') THEN
		CREATE DOMAIN money_in_eur AS text CHECK (VALUE ~ '^\d+(\.\d+)?$');
	END IF;
END$$;"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `jobs` table.
async fn init_jobs<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS jobs
		(
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			client_id bigint NOT NULL REFERENCES organizations(id),
			date_close timestamp,
			date_open timestamp NOT NULL,
			increment interval NOT NULL,
			invoice_date_issued timestamp,
			invoice_date_paid timestamp,
			invoice_hourly_rate money_in_eur NOT NULL,
			notes text NOT NULL,
			objectives text NOT NULL,

			CONSTRAINT jobs__date_integrity CHECK
			(
				(date_close IS null OR date_close > date_open) AND
				(invoice_date_issued IS null OR (date_close IS NOT null AND invoice_date_issued > date_close)) AND
				(invoice_date_paid IS null OR
					(invoice_date_issued IS NOT null AND invoice_date_paid > invoice_date_issued))
			)
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `timesheets` table.
async fn init_timesheets<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS timesheets
		(
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			employee_id bigint NOT NULL REFERENCES employees(id),
			job_id bigint NOT NULL REFERENCES jobs(id),
			time_begin timestamp NOT NULL,
			time_end timestamp,
			work_notes text NOT NULL,

			CONSTRAINT timesheets__date_integrity CHECK (time_begin < time_end),
			CONSTRAINT timesheets__employee_job_time_uq UNIQUE (employee_id, job_id, time_begin)
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

/// Initialize the `expenses` table.
async fn init_expenses<'connection, Conn>(connection: Conn) -> Result<()>
where
	Conn: Executor<'connection, Database = Postgres>,
{
	sqlx::query!(
		"CREATE TABLE IF NOT EXISTS expenses
		(
			id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			timesheet_id bigint NOT NULL REFERENCES timesheets(id) ON DELETE CASCADE,
			category text NOT NULL,
			cost money_in_eur NOT NULL,
			description text NOT NULL
		);"
	)
	.execute(connection)
	.await?;
	Ok(())
}

#[async_trait::async_trait]
impl Initializable for PgSchema
{
	type Db = Postgres;

	async fn init<'connection, Conn>(connection: Conn) -> Result<()>
	where
		Conn: Acquire<'connection, Database = Self::Db> + Send,
	{
		let mut transaction = connection.begin().await?;

		init_locations(&mut transaction).await?;
		init_organizations(&mut transaction).await?;
		init_contact_info(&mut transaction).await?;
		init_employees(&mut transaction).await?;
		init_money(&mut transaction).await?;
		init_jobs(&mut transaction).await?;
		init_timesheets(&mut transaction).await?;
		init_expenses(&mut transaction).await?;

		transaction.commit().await
	}
}
