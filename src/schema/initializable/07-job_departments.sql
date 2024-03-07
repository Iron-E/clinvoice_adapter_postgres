CREATE TABLE IF NOT EXISTS job_departments
(
	department_id uuid REFERENCES departments(id) ON DELETE CASCADE,
	job_id uuid REFERENCES jobs(id) ON DELETE CASCADE,
	PRIMARY KEY (department_id, job_id)
);
