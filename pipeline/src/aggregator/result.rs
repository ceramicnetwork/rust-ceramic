use std::{ops::Not as _, vec};

/// A result triple to precisely communicate the result of a validation operation.
/// Use [`fail!`] to early return from the method with the [`ValidationResult::Fail`] variant.
/// Use [`maybe_fail!`] to possibly early return from a method on failure or internal error, similar to the `?` operator.
#[must_use = "the validation result must not be forgotten"]
#[derive(Debug)]
pub enum ValidationResult<T = ()> {
    /// Validation has passed without any issues.
    /// A generic value may be carried along with the validation result.
    Pass(T),
    /// Validation found errors
    Fail(ValidationErrors),
    /// Validation itself encountered an error an cannot continue with the current record or future
    /// records in the batch.
    /// This error is not to be associated with any partiticular data record nor shared with the
    /// user. Rather this error is to be communicated to operators of the node.
    InternalError(anyhow::Error),
}

#[cfg(test)]
impl<T> ValidationResult<T> {
    pub fn expect(self, msg: &str) {
        match self {
            ValidationResult::Pass(_) => {}
            ValidationResult::Fail(validation_errors) => {
                panic!("validation failure: {msg}, {validation_errors}")
            }
            ValidationResult::InternalError(error) => {
                panic!("validation internal error: {msg}, {error}")
            }
        }
    }
    pub fn unwrap(self) -> T {
        match self {
            ValidationResult::Pass(v) => v,
            ValidationResult::Fail(validation_errors) => {
                panic!("validation failure: {validation_errors}")
            }
            ValidationResult::InternalError(error) => panic!("validation internal error: {error}"),
        }
    }
}

impl ValidationResult<()> {
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (ValidationResult::Pass(_), r) | (r, ValidationResult::Pass(_)) => r,
            (ValidationResult::Fail(mut s), ValidationResult::Fail(mut o)) => {
                s.0.append(&mut o.0);
                ValidationResult::Fail(s)
            }
            (
                ValidationResult::InternalError(error),
                ValidationResult::Fail(_validation_errors),
            )
            | (
                ValidationResult::Fail(_validation_errors),
                ValidationResult::InternalError(error),
            ) => ValidationResult::InternalError(error),
            (ValidationResult::InternalError(s), ValidationResult::InternalError(_o)) => {
                // Prefer the first error.
                ValidationResult::InternalError(s)
            }
        }
    }
}

impl From<Vec<String>> for ValidationResult<()> {
    fn from(value: Vec<String>) -> Self {
        value
            .is_empty()
            .not()
            .then(|| ValidationResult::Fail(value.into()))
            .unwrap_or(ValidationResult::Pass(()))
    }
}

/// Trait to map the binary pass/fail to the tertiary of pass/fail/error of validation
/// results.
pub trait ResultValidation<T> {
    fn map_to_validation_failure(self) -> ValidationResult<T>;
    fn map_to_validation_internal_err(self) -> ValidationResult<T>;
}
impl<T, E> ResultValidation<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
    anyhow::Error: From<E>,
{
    fn map_to_validation_failure(self) -> ValidationResult<T> {
        match self {
            Ok(v) => ValidationResult::Pass(v),
            Err(err) => ValidationResult::Fail(vec![err.to_string()].into()),
        }
    }

    fn map_to_validation_internal_err(self) -> ValidationResult<T> {
        match self {
            Ok(v) => ValidationResult::Pass(v),
            Err(err) => ValidationResult::InternalError(anyhow::Error::from(err)),
        }
    }
}

/// Trait for asserting an optional value exists or mapping to a tertiary validation result.
pub trait OptionValidation<T> {
    fn ok_or_validation_failure(self, msg: &str) -> ValidationResult<T>;
    fn ok_or_validation_internal_err(self, msg: &str) -> ValidationResult<T>;
}

impl<T> OptionValidation<T> for Option<T> {
    fn ok_or_validation_failure(self, msg: &str) -> ValidationResult<T> {
        match self {
            Some(v) => ValidationResult::Pass(v),
            None => ValidationResult::Fail(vec![msg.to_string()].into()),
        }
    }

    fn ok_or_validation_internal_err(self, msg: &str) -> ValidationResult<T> {
        match self {
            Some(v) => ValidationResult::Pass(v),
            None => ValidationResult::InternalError(anyhow::anyhow!("{msg}")),
        }
    }
}

/// A set of validations errors.
#[must_use = "these validation errors must not be forgotten"]
#[derive(Debug, Default)]
pub struct ValidationErrors(pub Vec<String>);

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<Vec<String>> for ValidationErrors {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}
impl IntoIterator for ValidationErrors {
    type Item = String;

    type IntoIter = std::vec::IntoIter<String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Return early with the provided values as error strings.
macro_rules! fail {
    ($msg:literal $(,)?) => {
        return $crate::aggregator::result::ValidationResult::Fail($crate::aggregator::result::ValidationErrors(vec![format!($msg)]))
    };
    ($errs:expr $(,)?) => {
        return $crate::aggregator::result::ValidationResult::Fail($errs.into())
    };
    ($fmt:expr, $($arg:tt)*) => {
        return $crate::aggregator::result::ValidationResult::Fail($crate::aggregator::result::ValidationErrors(vec![format!($fmt, $($arg)*)]))
    };
    ([$fmt:expr, $($arg:tt)*]*) => {
        return $crate::aggregator::result::ValidationResult::Fail($crate::aggregator::result::ValidationErrors(vec![$(format!($fmt, $($arg)*))*]))
    };
}
// Check the expression and early return if its a validation fail.
// Otherwise produce the value of the pass variant.
macro_rules! maybe_fail {
    ($result:expr $(,)?) => {
        match $result {
            $crate::aggregator::result::ValidationResult::Pass(v) => v,
            $crate::aggregator::result::ValidationResult::Fail(errs) => {
                return $crate::aggregator::result::ValidationResult::Fail(errs)
            }
            $crate::aggregator::result::ValidationResult::InternalError(err) => {
                return $crate::aggregator::result::ValidationResult::InternalError(err)
            }
        }
    };
}
