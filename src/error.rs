use failure::Fail;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Sqlite Error: {:?}", _0)]
    Sql(rusqlite::Error),
    #[fail(display = "Sqlite Connection Pool Error: {:?}", _0)]
    ConnectionPool(r2d2::Error),
    #[fail(display = "{:?}", _0)]
    Failure(failure::Error),
    #[fail(display = "Sqlite: Connection closed")]
    SqlNoConnection,
    #[fail(display = "Sqlite: Already open")]
    SqlAlreadyOpen,
    #[fail(display = "Sqlite: Failed to open")]
    SqlFailedToOpen,
    #[fail(display = "{:?}", _0)]
    Io(std::io::Error),
    #[fail(display = "{:?}", _0)]
    Message(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Error {
        Error::Sql(err)
    }
}

impl From<failure::Error> for Error {
    fn from(err: failure::Error) -> Error {
        Error::Failure(err)
    }
}

impl From<r2d2::Error> for Error {
    fn from(err: r2d2::Error) -> Error {
        Error::ConnectionPool(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($crate::error::Error::Message($e.to_string()));
    };
    ($fmt:expr, $($arg:tt)+) => {
        return Err($crate::error::Error::Message(format!($fmt, $($arg)+)));
    };
}

#[macro_export]
macro_rules! format_err {
    ($e:expr) => {
        $crate::error::Error::Message($e.to_string());
    };
    ($fmt:expr, $($arg:tt)+) => {
        $crate::error::Error::Message(format!($fmt, $($arg)+));
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            bail!($fmt, $($arg)+);
        }
    };
}

#[macro_export]
macro_rules! ensure_eq {
    ($left:expr, $right:expr) => ({
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    bail!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`"#, left_val, right_val)
                }
            }
        }
    });
    ($left:expr, $right:expr,) => ({
        ensure_eq!($left, $right)
    });
    ($left:expr, $right:expr, $($arg:tt)+) => ({
        match (&($left), &($right)) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    bail!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}"#, left_val, right_val,
                           format_args!($($arg)+))
                }
            }
        }
    });
}
