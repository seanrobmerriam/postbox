//! HTTP and gRPC front ends for Postbox.

pub mod grpc;
pub mod http;

pub use http::{router, AppState, HttpError, HttpResult, init_prometheus};