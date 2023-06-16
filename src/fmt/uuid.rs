mod display;
mod from;

use winvoice_schema::Id;

/// Has a [`Display`] impl which will generate valid syntax to represent the given
/// [`NaiveDateTime`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PgUuid(Id);
