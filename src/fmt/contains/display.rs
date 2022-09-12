use core::fmt::{Display, Formatter, Result};

use super::PgContains;

impl Display for PgContains<'_>
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result
	{
		write!(f, "%{}%", self.0)
	}
}
