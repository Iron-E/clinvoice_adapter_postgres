use super::{Currency, PgCurrency};

impl From<Currency> for PgCurrency
{
	fn from(currency: Currency) -> Self
	{
		Self(currency)
	}
}
