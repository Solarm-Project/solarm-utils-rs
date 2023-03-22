use miette::Diagnostic;
use std::string::FromUtf8Error;
use thiserror::Error;

type Result<T> = miette::Result<T, Error>;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error(transparent)]
    ZfsBuilderError(#[from] zfs::ZfsBuilderError),

    #[error("zfs process failed: {0}")]
    ZFSError(String),

    #[error("zpool process failed: {0}")]
    ZpoolError(String),

    #[error("{0} is not a supported list type must be either: filesystem, snapshot, volume, bookmark or all")]
    InvalidZfsListType(String),
}

pub mod zfs;

#[cfg(test)]
mod tests {

    #[test]
    fn builder_works() -> miette::Result<()> {
        let ds = crate::zfs::create(
            &crate::zfs::CreateRequestBuilder::default()
                .name("testvol")
                .add_property("blub", "test")
                .volsize("10G")
                .build()?,
        )?;

        assert_eq!("testvol", ds.name());

        let ds = crate::zfs::create(
            &crate::zfs::CreateRequestBuilder::default()
                .name("testds")
                .build()?,
        )?;

        assert_eq!("testds", ds.name());

        let _list = crate::zfs::list(&crate::zfs::ListRequestBuilder::default().build()?)?;

        Ok(())
    }

    #[test]
    fn ds_tests() -> miette::Result<()> {
        let ds = crate::zfs::open("testds")?;

        assert_eq!("testds", ds.name());

        assert!(ds.promote().is_ok());

        assert!(ds.destroy().is_ok());

        Ok(())
    }
}
