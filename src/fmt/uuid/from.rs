use super::{Id, PgUuid};

impl From<Id> for PgUuid
{
	fn from(id: Id) -> Self
	{
		Self(id)
	}
}
