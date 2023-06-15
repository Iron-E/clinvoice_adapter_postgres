use core::fmt::{Display, Formatter, Result};

use super::PgUuid;

impl Display for PgUuid
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result
	{
		write!(f, "'{}'::uuid", self.0)
	}
}
