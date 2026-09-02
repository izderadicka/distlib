//! JSON-RPC 2.0, the parts of it this API uses.
//!
//! Hand-rolled rather than pulled from a crate: the envelope is three structs
//! and a handful of error codes, and every crate that offers it also offers a
//! transport, a router and a macro layer we would then be fitting axum around.
//!
//! Deliberately not implemented: batch requests and notifications. Nothing here
//! is chatty enough to need batching, and every method this API has is one a
//! caller wants an answer to. A batch arrives as an array where an object is
//! expected and is refused as an invalid request, which is the honest answer —
//! better than accepting one and silently running only its first element.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON-RPC request.
#[derive(Debug, Deserialize)]
pub struct Request {
    /// Must be exactly `"2.0"`.
    pub jsonrpc: String,
    /// Echoed back on the response. Absent means a notification, which this
    /// API does not serve — see the module docs.
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

/// A JSON-RPC response: exactly one of `result` or `error`.
#[derive(Debug, Serialize)]
pub struct Response {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

impl Response {
    pub fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn failed(id: Value, error: Error) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// A JSON-RPC error object.
#[derive(Debug, Serialize, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Error {
    /// The request was not a well-formed JSON-RPC call.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(-32600, message)
    }

    /// No such method.
    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("no such method: {method}"))
    }

    /// The params were missing, malformed, or the wrong shape.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }

    /// The method ran and failed.
    ///
    /// -32000 rather than -32603: the spec reserves -32603 for a fault in the
    /// server itself, while a refused proposal or an unreachable leader is this
    /// method working correctly and reporting what happened.
    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(-32000, message)
    }

    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}
