use std::{
    error::Error as StdError,
    fmt::Display,
    io,
    net::AddrParseError,
};

use crate::object_storage;

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub enum ExitCode
{
    Success,
    InvalidSocketAddress,
    AddressAlreadyInUse,
    StorageFileAlreadyExists,
    StorageFileNotExists,
    PermissionDenied,
    Other(i32),
}

impl ExitCode
{
    pub fn into_raw(&self) -> i32
    {
        match self {
            Self::Success => 0,
            Self::InvalidSocketAddress => 1,
            Self::AddressAlreadyInUse => 2,
            Self::StorageFileAlreadyExists => 3,
            Self::StorageFileNotExists => 4,
            Self::PermissionDenied => 5,
            Self::Other(code) => *code,
        }
    }

    #[allow(dead_code)]
    pub fn from_raw(code: i32) -> Self
    {
        match code {
            0 => Self::Success,
            1 => Self::InvalidSocketAddress,
            2 => Self::AddressAlreadyInUse,
            3 => Self::StorageFileAlreadyExists,
            4 => Self::StorageFileNotExists,
            5 => Self::PermissionDenied,
            code => Self::Other(code),
        }
    }
}

impl Into<i32> for ExitCode
{
    fn into(self) -> i32
    {
        self.into_raw()
    }
}

impl Display for ExitCode
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        write!(f, "{}", self.into_raw())
    }
}

#[derive(Debug)]
pub struct Error(pub ExitCode, pub String);

impl Error
{
    pub fn exit_code(&self) -> ExitCode
    {
        self.0
    }
}

impl Display for Error
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        write!(f, "{}", self.1)
    }
}

impl StdError for Error
{
    fn source(&self) -> Option<&(dyn StdError + 'static)>
    {
        None
    }
}

impl From<rusqlite::Error> for Error
{
    fn from(error: rusqlite::Error) -> Self
    {
        Error(ExitCode::Other(128), error.to_string())
    }
}

impl From<refinery::Error> for Error
{
    fn from(error: refinery::Error) -> Self
    {
        Error(ExitCode::Other(128), error.to_string())
    }
}

impl From<AddrParseError> for Error
{
    fn from(error: AddrParseError) -> Self
    {
        Error(ExitCode::InvalidSocketAddress, error.to_string())
    }
}

impl From<object_storage::Error> for Error
{
    fn from(error: object_storage::Error) -> Self
    {
        Error(ExitCode::Other(5), error.to_string())
    }
}

impl From<io::Error> for Error
{
    fn from(error: io::Error) -> Self
    {
        Error(ExitCode::Other(5), error.to_string())
    }
}

impl From<tonic::transport::Error> for Error
{
    fn from(error: tonic::transport::Error) -> Self
    {
        Error(ExitCode::Other(5), error.to_string())
    }
}
