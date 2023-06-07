use core::fmt::{Display, Formatter, Result};

use super::PgTimestampTz;

impl Display for PgTimestampTz
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result
	{
		write!(f, "TIMESTAMP '{}'", self.0)
	}
}
