use std::collections::BTreeMap;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use validator::ValidationErrors;

/// 统一业务错误：通过 `IntoResponse` 映射为 HTTP 状态码与错误体，
/// 映射关系见 docs/DEVELOPMENT_DESIGN.md §5.1 / §6.3。
#[derive(Debug)]
pub enum SimError {
    /// 请求语义非法（action/order_type/reserve_price 等）→ 400
    BadRequest { detail: String },
    /// 字段校验失败 → 422
    Validation { fields: BTreeMap<String, String> },
    /// order_id 重复 → 409
    DuplicateOrderId,
    /// 撮合核心不可用（命令通道关闭/停机中）→ 503
    CoreUnavailable,
    /// 等待撮合结果超时 → 504
    CoreTimeout,
}

impl SimError {
    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::BadRequest {
            detail: detail.into(),
        }
    }

    pub fn from_validation(errors: ValidationErrors) -> Self {
        let fields = errors
            .field_errors()
            .iter()
            .map(|(field, errs)| {
                (
                    (*field).to_string(),
                    errs.iter()
                        .map(|e| e.code.to_string())
                        .collect::<Vec<_>>()
                        .join("; "),
                )
            })
            .collect::<BTreeMap<_, _>>();
        Self::Validation { fields }
    }
}

impl IntoResponse for SimError {
    fn into_response(self) -> Response {
        let (status, body): (StatusCode, serde_json::Value) = match self {
            SimError::BadRequest { detail } => (
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": "invalid_request", "detail": detail }),
            ),
            SimError::Validation { fields } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                serde_json::json!({ "error": "validation", "fields": fields }),
            ),
            SimError::DuplicateOrderId => (
                StatusCode::CONFLICT,
                serde_json::json!({ "error": "duplicate_order_id" }),
            ),
            SimError::CoreUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({ "error": "core_unavailable" }),
            ),
            SimError::CoreTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                serde_json::json!({ "error": "core_timeout" }),
            ),
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(err: SimError) -> StatusCode {
        err.into_response().status()
    }

    #[test]
    fn test_error_status_mapping() {
        assert_eq!(
            status_of(SimError::bad_request("x")),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_of(SimError::Validation {
                fields: BTreeMap::new()
            }),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(status_of(SimError::DuplicateOrderId), StatusCode::CONFLICT);
        assert_eq!(
            status_of(SimError::CoreUnavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_of(SimError::CoreTimeout),
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[test]
    fn test_from_validation_collects_fields() {
        use validator::Validate;

        #[derive(validator::Validate)]
        struct Demo {
            #[validate(range(min = 1))]
            size: i64,
        }
        let demo = Demo { size: 0 };
        let SimError::Validation { fields } =
            SimError::from_validation(demo.validate().unwrap_err())
        else {
            panic!("expected Validation");
        };
        assert!(fields.contains_key("size"));
    }
}
