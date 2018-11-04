//! Types module for the main runtime, exposing error and result types.
use quick_xml::events::Event;
use quick_xml::Reader;
use rusoto_core::request;
use std::fmt::{self, Debug, Display, Formatter};
use std::{io, time};

/// Public type alias for a result with a `ConcatResult` error type.
pub type ConcatResult<T> = Result<T, ConcatResult>;

/// Delegating error wrapper for errors raised by the main archive.
///
/// The internal `String` representation enables cheap coercion from
/// other error types by binding their error messages through. This
/// is somewhat similar to the `failure` crate, but minimal.
pub struct ConcatResult(String);

/// Debug implementation for `ConcatResult`.
impl Debug for ConcatResult {
    /// Formats an `ConcatResult` by delegating to `Display`.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(self, f)
    }
}

/// Display implementation for `ConcatResult`.
impl Display for ConcatResult {
    /// Formats an `ConcatResult` by writing out the inner representation.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Macro to implement `From` for provided types.
macro_rules! derive_from {
    ($type:ty) => {
        impl<'a> From<$type> for ConcatResult {
            fn from(t: $type) -> ConcatResult {
                ConcatResult(t.to_string())
            }
        }
    };
}

// Easy derivations of derive_from.
derive_from!(&'a str);
derive_from!(io::Error);
derive_from!(regex::Error);
derive_from!(request::TlsError);
derive_from!(time::SystemTimeError);
derive_from!(String);

/// Macro to implement `From` for Rusoto types.
macro_rules! derive_from_rusoto {
    ($type:ty) => {
        impl From<$type> for ConcatResult {
            /// Converts a Rusoto error to a `ConcatResult`.
            fn from(err: $type) -> ConcatResult {
                // grab the raw conversion
                let msg = err.to_string();

                // XML, look for a message!
                if msg.starts_with("<?xml") {
                    // create an XML reader and buffer
                    let mut reader = Reader::from_str(&msg);
                    let mut buffer = Vec::new();

                    loop {
                        // parse through each XML node event
                        match reader.read_event(&mut buffer) {
                            // end, or error, just give up
                            Ok(Event::Eof) | Err(_) => break,

                            // if we find a message tag, we'll use that as the error
                            Ok(Event::Start(ref e)) if e.name() == b"Message" => {
                                return ConcatResult(
                                    reader
                                        .read_text(b"Message", &mut Vec::new())
                                        .expect("Cannot decode text value"),
                                )
                            }

                            // skip
                            _ => (),
                        }
                        // empty buffers
                        buffer.clear();
                    }
                }

                // default msg
                ConcatResult(msg)
            }
        }
    };
}

// derive error display for all used rusoto_s3 types
derive_from_rusoto!(rusoto_s3::AbortMultipartUploadError);
derive_from_rusoto!(rusoto_s3::CompleteMultipartUploadError);
derive_from_rusoto!(rusoto_s3::CreateMultipartUploadError);
derive_from_rusoto!(rusoto_s3::DeleteObjectError);
derive_from_rusoto!(rusoto_s3::ListObjectsV2Error);
derive_from_rusoto!(rusoto_s3::ListPartsError);
derive_from_rusoto!(rusoto_s3::UploadPartCopyError);

#[cfg(test)]
mod tests {
    use super::ConcatResult;
    use rusoto_core::credential::CredentialsError;
    use rusoto_s3::ListObjectsV2Error;
    use std::io::{Error, ErrorKind};

    #[test]
    fn converting_io_to_error() {
        let message = "My fake access key failed message";
        let io_errs = Error::new(ErrorKind::Other, message);
        let convert = ConcatResult::from(io_errs);

        assert_eq!(convert.0, message);
    }

    #[test]
    fn converting_rusoto_to_error() {
        let message = "My fake access key failed message".to_string();
        let xml_err = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>InvalidAccessKeyId</Code>
                    <Message>{}</Message>
                </Error>"#,
            message
        );

        let creds_err = CredentialsError::new(xml_err);
        let lists_err = ListObjectsV2Error::Credentials(creds_err);
        let converted = ConcatResult::from(lists_err);

        assert_eq!(converted.0, message);
    }

    #[test]
    fn converting_string_to_error() {
        let message = "My fake access key failed message".to_string();
        let convert = ConcatResult::from(message.clone());

        assert_eq!(convert.0, message);
    }

    #[test]
    fn converting_str_to_error() {
        let message = "My fake access key failed message";
        let convert = ConcatResult::from(message);

        assert_eq!(convert.0, message);
    }
}
