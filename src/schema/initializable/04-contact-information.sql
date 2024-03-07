CREATE TABLE IF NOT EXISTS contact_information
(
	label text NOT NULL PRIMARY KEY,

	address_id uuid REFERENCES locations(id),
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
);
