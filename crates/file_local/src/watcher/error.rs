use std::fmt;

#[derive(Debug)]
pub enum Error {
    NotifyError(notify::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)?;
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self {
            Error::NotifyError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Self {
        Self::NotifyError(err)
    }
}
