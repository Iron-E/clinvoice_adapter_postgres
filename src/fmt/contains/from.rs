use super::PgContains;

impl<'sub> From<&'sub str> for PgContains<'sub>
{
	fn from(s: &'sub str) -> Self
	{
		Self(s)
	}
}

impl<'sub> From<&'sub String> for PgContains<'sub>
{
	fn from(s: &'sub String) -> Self
	{
		Self::from(s.as_str())
	}
}
