CREATE TABLE IF NOT EXISTS timesheets
(
	id uuid NOT NULL PRIMARY KEY,
	employee_id uuid NOT NULL REFERENCES employees(id),
	job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
	time_begin timestamp NOT NULL,
	time_end timestamp,
	work_notes text NOT NULL,

	CONSTRAINT timesheets__date_integrity CHECK (time_begin < time_end),
	CONSTRAINT timesheets__employee_job_time_uq UNIQUE (employee_id, job_id, time_begin)
);
