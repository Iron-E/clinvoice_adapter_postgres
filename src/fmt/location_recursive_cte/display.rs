use core::fmt::{Display, Formatter, Result};

use super::PgLocationRecursiveCte;

impl<T, Outer> Display for PgLocationRecursiveCte<T, Outer>
where
	T: Display,
	Outer: Display,
{
	fn fmt(&self, f: &mut Formatter<'_>) -> Result
	{
		self.0.fmt(f)
	}
}
