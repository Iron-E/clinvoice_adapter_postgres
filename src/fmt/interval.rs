mod display;
mod from;

use core::time::Duration;

/// Has a [`Display`] impl which will generate valid syntax to represent the given [`Duration`].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PgInterval(Duration);
