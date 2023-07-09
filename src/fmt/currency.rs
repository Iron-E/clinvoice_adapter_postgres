mod display;
mod from;

use winvoice_schema::Currency;

/// Has a [`Display`] impl which will generate valid syntax to represent the given [`Duration`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PgCurrency(Currency);
