use super::{NaiveDateTime, PgTimestampTz};

impl From<NaiveDateTime> for PgTimestampTz
{
	fn from(date: NaiveDateTime) -> Self
	{
		Self(date)
	}
}
