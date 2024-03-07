CREATE TABLE IF NOT EXISTS locations
(
	currency text,
	id uuid NOT NULL PRIMARY KEY,
	outer_id uuid REFERENCES locations(id),
	name text NOT NULL,

	CONSTRAINT locations__not_outside_self CHECK (id <> outer_id)
);
