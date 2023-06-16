mod display;
mod from;

/// Implements [`Display`] such that the inner field will be represented as a substring (e.g.
/// `'ABC' LIKE '%B%'`, i.e. `PgContains("B")`.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PgContains<'sub>(&'sub str);
