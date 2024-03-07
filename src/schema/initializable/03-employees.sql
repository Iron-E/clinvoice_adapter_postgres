CREATE TABLE IF NOT EXISTS employees
(
	active boolean NOT NULL,
	department_id uuid NOT NULL REFERENCES departments(id),
	id uuid NOT NULL PRIMARY KEY,
	name text NOT NULL,
	title text NOT NULL
);
