use std::collections::BTreeMap;

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use validator::ValidationErrors;

/// 统一业务错误：通过 `IntoResponse` 映射为 HTTP 状态码与 JSON 错误体，
/// 映射关系见 docs/DEVELOPMENT_DESIGN.md §5.1 / §6.3。
#[derive(Debug)]
pub enum SimError {
    /// 请求无法解析或语义非法（action/order_type/reserve_price 等）→ 400；
    /// 提取器拒绝时沿用其原状态码（如缺 Content-Type 的 415）
    BadRequest { status: StatusCode, detail: String },
    /// 目标订单不存在（从未下达或已被注册表容量淘汰）→ 404
    OrderNotFound,
    /// 字段校验失败 → 422
    Validation { fields: BTreeMap<String, String> },
    /// order_id 重复 → 409
    DuplicateOrderId,
    /// 同一订单已有撤单请求在途 → 409
    CancelInFlight,
    /// 撮合核心不可用（命令通道关闭/停机中）→ 503
    CoreUnavailable,
    /// 等待撮合结果超时 → 504
    CoreTimeout,
}

impl SimError {
    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::BadRequest {
            status: StatusCode::BAD_REQUEST,
            detail: detail.into(),
        }
    }

    /// 提取器拒绝（JSON/Path/Query）→ 统一 JSON 错误体，保留原状态码与 axum 描述
    fn rejected(status: StatusCode, detail: String) -> Self {
        Self::BadRequest { status, detail }
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

impl From<JsonRejection> for SimError {
    fn from(rejection: JsonRejection) -> Self {
        Self::rejected(rejection.status(), rejection.body_text())
    }
}

impl From<PathRejection> for SimError {
    fn from(rejection: PathRejection) -> Self {
        Self::rejected(rejection.status(), rejection.body_text())
    }
}

impl From<QueryRejection> for SimError {
    fn from(rejection: QueryRejection) -> Self {
        Self::rejected(rejection.status(), rejection.body_text())
    }
}

impl IntoResponse for SimError {
    fn into_response(self) -> Response {
        let (status, body): (StatusCode, serde_json::Value) = match self {
            SimError::BadRequest { status, detail } => (
                status,
                serde_json::json!({ "error": "invalid_request", "detail": detail }),
            ),
            SimError::OrderNotFound => (
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "order_not_found" }),
            ),
            SimError::Validation { fields } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                serde_json::json!({ "error": "validation", "fields": fields }),
            ),
            SimError::DuplicateOrderId => (
                StatusCode::CONFLICT,
                serde_json::json!({ "error": "duplicate_order_id" }),
            ),
            SimError::CancelInFlight => (
                StatusCode::CONFLICT,
                serde_json::json!({ "error": "cancel_in_flight" }),
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
        assert_eq!(status_of(SimError::OrderNotFound), StatusCode::NOT_FOUND);
        assert_eq!(status_of(SimError::DuplicateOrderId), StatusCode::CONFLICT);
        assert_eq!(status_of(SimError::CancelInFlight), StatusCode::CONFLICT);
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
    fn test_rejection_status_and_json_body_preserved() {
        // 提取器拒绝走的是同一条 BadRequest 通道：状态码可非 400，但错误体仍是统一 JSON
        let response = SimError::rejected(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Expected request with `Content-Type: application/json`".to_string(),
        )
        .into_response();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
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
