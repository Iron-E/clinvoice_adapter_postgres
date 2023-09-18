#![allow(clippy::std_instead_of_core)]

use core::fmt::Write;
use std::{borrow::Cow, collections::BTreeSet, error::Error as StdError};

use sqlx::error::DatabaseError;
use thiserror::Error;
use winvoice_schema::Id;

#[derive(Clone, Debug, Default, Eq, Error, Hash, Ord, PartialEq, PartialOrd)]
#[error("{message}")]
pub struct ContactInformationCheckViolation
{
	message: String,
}

impl ContactInformationCheckViolation
{
	pub fn new(ids: BTreeSet<Id>) -> Self
	{
		let mut iter = ids.iter();
		let mut message = format!(
			"Part of deletion operation on table `locations` violates constraint `contact_information__is_variant`. \
			 IDs skipped: {}",
			iter.next().unwrap()
		);

		iter.try_for_each(|id| write!(message, ", {id}")).unwrap();
		Self { message }
	}
}

impl DatabaseError for ContactInformationCheckViolation
{
	fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static)
	{
		self
	}

	fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static)
	{
		self
	}

	fn code(&self) -> Option<Cow<'_, str>>
	{
		Some("CHECK_VIOLATION".into())
	}

	fn constraint(&self) -> Option<&str>
	{
		Some("contact_information__is_variant")
	}

	fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static>
	{
		self
	}

	fn is_transient_in_connect_phase(&self) -> bool
	{
		false
	}

	fn message(&self) -> &str
	{
		self.message.as_str()
	}
}
