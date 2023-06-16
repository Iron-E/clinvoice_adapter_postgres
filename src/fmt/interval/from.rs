use winvoice_match::Serde;

use super::{Duration, PgInterval};

impl From<Duration> for PgInterval
{
	fn from(duration: Duration) -> Self
	{
		Self(duration)
	}
}

impl From<Serde<Duration>> for PgInterval
{
	fn from(duration: Serde<Duration>) -> Self
	{
		Self::from(duration.into_inner())
	}
}
