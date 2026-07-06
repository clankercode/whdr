//! HTTP ingest plane: provider webhooks in, extension dispatch, reply out.
//!
//! Server-generated responses (404/413/429/503/504) follow SPEC §4.1;
//! everything else is the extension's `Result.http` passed through verbatim.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::header::{HeaderName, HeaderValue, RETRY_AFTER};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use whdr_proto::HttpReply;

use crate::daemon::{AppState, DispatchError};

pub fn route_key_from_path(path: &str) -> Option<String> {
    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_string)
}

pub(crate) async fn ingest_handler(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let config = state.config().await;
    if body.len() > config.limits.max_body_bytes {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let Some(route_key) = route_key_from_path(uri.path()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let headers = header_map_to_btree(&headers);
    let query = uri.query().map(ToString::to_string);
    match state
        .dispatch(
            &route_key,
            method,
            uri.path().to_string(),
            query,
            headers,
            body,
        )
        .await
    {
        Ok(result) => http_reply_to_response(result.http),
        Err(DispatchError::Busy) => StatusCode::TOO_MANY_REQUESTS.into_response(),
        Err(DispatchError::Starting) => {
            let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        Err(DispatchError::Timeout) => StatusCode::GATEWAY_TIMEOUT.into_response(),
        Err(DispatchError::Dead) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(DispatchError::NotFound) => StatusCode::NOT_FOUND.into_response(),
    }
}

fn header_map_to_btree(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect()
}

fn http_reply_to_response(reply: HttpReply) -> Response {
    let status = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, reply.body).into_response();
    for (name, value) in reply.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}
