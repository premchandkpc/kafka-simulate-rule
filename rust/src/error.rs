use std::fmt;

#[derive(Debug)]
pub enum Error {
    Lex(super::dsl::lexer::LexError),
    Parse(super::dsl::parser::ParseError),
    Compile(super::dsl::compiler::CompileError),
    Exec(String),
    Ffi(FfiError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiError {
    NullPointer = -1,
    InvalidUtf8 = -2,
    Lex = -3,
    Parse = -4,
    Compile = -5,
    Serialize = -6,
    BufferTooSmall = -7,
    Deserialize = -8,
    Exec = -9,
}

impl FfiError {
    pub fn code(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Lex(e) => write!(f, "lex error: {}", e),
            Error::Parse(e) => write!(f, "parse error: {}", e),
            Error::Compile(e) => write!(f, "compile error: {}", e),
            Error::Exec(e) => write!(f, "exec error: {}", e),
            Error::Ffi(e) => write!(f, "ffi error: {:?}", e),
        }
    }
}

impl std::error::Error for Error {}
