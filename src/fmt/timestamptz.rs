mod display;

use winvoice_schema::chrono::NaiveDateTime;

/// Has a [`Display`] impl which will generate valid syntax to represent the given
/// [`NaiveDateTime`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PgTimestampTz(pub(crate) NaiveDateTime);
