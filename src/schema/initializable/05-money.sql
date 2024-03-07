DO $$
BEGIN
	IF NOT EXISTS (SELECT FROM pg_type WHERE typname = 'money_in_eur') THEN
		CREATE DOMAIN money_in_eur AS text CHECK (VALUE ~ '^\d+(\.\d+)?$');
	END IF;
END$$;
