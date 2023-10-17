use winvoice_schema::chrono::{DateTime, TimeZone};

use super::{NaiveDateTime, PgTimestampTz};

impl<Tz> From<DateTime<Tz>> for PgTimestampTz
where
	Tz: TimeZone,
{
	fn from(date: DateTime<Tz>) -> Self
	{
		Self::from(date.naive_local())
	}
}

impl From<NaiveDateTime> for PgTimestampTz
{
	fn from(date: NaiveDateTime) -> Self
	{
		Self(date)
	}
}
