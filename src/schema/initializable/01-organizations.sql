CREATE TABLE IF NOT EXISTS organizations
(
	id uuid NOT NULL PRIMARY KEY,
	location_id uuid NOT NULL REFERENCES locations(id),
	name text NOT NULL
);
