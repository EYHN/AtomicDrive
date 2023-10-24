use thiserror::Error;
use crate::tracker::Error as TrackerError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Tracker error")]
    TrackerError(#[from] TrackerError),
    #[error("IO error")]
    IOError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
