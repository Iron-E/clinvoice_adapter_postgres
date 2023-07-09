use core::fmt::{Display, Formatter, Result};

use super::PgCurrency;

impl Display for PgCurrency
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result
	{
		write!(f, "'{}'", self.0)
	}
}
