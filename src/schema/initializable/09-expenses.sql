CREATE TABLE IF NOT EXISTS expenses
(
	id uuid NOT NULL PRIMARY KEY,
	timesheet_id uuid NOT NULL REFERENCES timesheets(id) ON DELETE CASCADE,
	category text NOT NULL,
	cost money_in_eur NOT NULL,
	description text NOT NULL
);
