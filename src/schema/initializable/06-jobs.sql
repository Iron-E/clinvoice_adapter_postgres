CREATE TABLE IF NOT EXISTS jobs
(
	id uuid NOT NULL PRIMARY KEY,
	client_id uuid NOT NULL REFERENCES organizations(id),
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
);
