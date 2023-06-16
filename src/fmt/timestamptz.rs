mod display;
mod from;

use winvoice_schema::chrono::NaiveDateTime;

/// Has a [`Display`] impl which will generate valid syntax to represent the given
/// [`NaiveDateTime`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PgTimestampTz(NaiveDateTime);
