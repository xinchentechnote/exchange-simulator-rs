//! SZSE 二进制帧编解码。
//!
//! 帧格式（大端序）：`msg_type(u32) + body_length(u32) + body + checksum(i32)`，
//! 固定头 8 字节（比 SSE 少 msg_seq_num），校验和 4 字节；校验和由 binary_codec
//! 全局注册的 `SzsebinChecksumService`（字节累加 mod 256）在 encode 时自动计算。

use binary_codec::BinaryCodec;
use bytes::BytesMut;
use szse_binary::szse_binary::{SzseBinary, SzseBinaryBodyEnum};

pub const HEADER_LEN: usize = 8;
pub const CHECKSUM_LEN: usize = 4;
/// 单帧 body 上限防护，超过直接断开连接
pub const MAX_BODY_LEN: u32 = 8 * 1024 * 1024;

// 报文类型码（SZSE 二进制协议）
pub const MSG_LOGON: u32 = 1;
pub const MSG_LOGOUT: u32 = 2;
pub const MSG_HEARTBEAT: u32 = 3;
pub const MSG_BUSINESS_REJECT: u32 = 4;
pub const MSG_NEW_ORDER: u32 = 100101;
/// 撤单请求（Applied 类）
pub const MSG_ORDER_CANCEL_REQUEST: u32 = 190007;
/// 成交确认（受理/撤单确认等）
pub const MSG_EXECUTION_CONFIRM: u32 = 200102;
/// 成交流水
pub const MSG_EXECUTION_REPORT: u32 = 200115;
pub const MSG_CANCEL_REJECT: u32 = 290008;

#[derive(Debug)]
pub enum FrameError {
    UnknownMessageType { msg_type: u32 },
    BodyTooLarge { body_len: u32 },
    MalformedBody,
}

/// 报文体 → 类型码（encode 需要显式回填 msg_type 字段）。
/// NewOrder/ExecutionConfirm 出站统一使用 100101 / 200102。
pub fn msg_type_of(body: &SzseBinaryBodyEnum) -> u32 {
    match body {
        SzseBinaryBodyEnum::Logon(_) => MSG_LOGON,
        SzseBinaryBodyEnum::Logout(_) => MSG_LOGOUT,
        SzseBinaryBodyEnum::Heartbeat(_) => MSG_HEARTBEAT,
        SzseBinaryBodyEnum::BusinessReject(_) => MSG_BUSINESS_REJECT,
        SzseBinaryBodyEnum::ReportSynchronization(_) => 5,
        SzseBinaryBodyEnum::PlatformStateInfo(_) => 6,
        SzseBinaryBodyEnum::ReportFinished(_) => 7,
        SzseBinaryBodyEnum::PlatformPartition(_) => 9,
        SzseBinaryBodyEnum::TradingSessionStatus(_) => 10,
        SzseBinaryBodyEnum::NewOrder(_) => MSG_NEW_ORDER,
        SzseBinaryBodyEnum::ExecutionConfirm(_) => MSG_EXECUTION_CONFIRM,
        SzseBinaryBodyEnum::ExecutionReport(_) => MSG_EXECUTION_REPORT,
        SzseBinaryBodyEnum::OrderCancelRequest(_) => MSG_ORDER_CANCEL_REQUEST,
        SzseBinaryBodyEnum::CancelReject(_) => MSG_CANCEL_REJECT,
    }
}

/// 从缓冲区尝试提取一帧。缓冲不足返回 `Ok(None)`。
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<SzseBinary>, FrameError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    // 布局：msg_type(0..4) + body_length(4..8)
    let body_len = u32::from_be_bytes(buf[4..HEADER_LEN].try_into().expect("切片长度恒为 4"));
    if body_len > MAX_BODY_LEN {
        return Err(FrameError::BodyTooLarge { body_len });
    }
    let total = HEADER_LEN + body_len as usize + CHECKSUM_LEN;
    if buf.len() < total {
        return Ok(None);
    }
    let msg_type = u32::from_be_bytes(buf[0..4].try_into().expect("切片长度恒为 4"));
    let mut frame = buf.split_to(total).freeze();
    // 报文体解码按固定布局读取，恶意 body_len 可能导致越界 panic——捕获后按坏报文处理
    let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        SzseBinary::decode(&mut frame)
    }));
    match decoded {
        Ok(Some(msg)) => Ok(Some(msg)),
        Ok(None) => Err(FrameError::UnknownMessageType { msg_type }),
        Err(_) => Err(FrameError::MalformedBody),
    }
}

/// 编码一帧（校验和由 binary_codec 全局服务自动计算）。
pub fn encode_frame(body: SzseBinaryBodyEnum, out: &mut BytesMut) {
    let msg_type = msg_type_of(&body);
    let msg = SzseBinary {
        msg_type,
        body_length: 0,
        body,
        checksum: 0,
    };
    msg.encode(out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use szse_binary::heartbeat::Heartbeat;
    use szse_binary::logon::Logon;

    fn sample_logon() -> SzseBinaryBodyEnum {
        SzseBinaryBodyEnum::Logon(Logon {
            sender_comp_id: "szoms01".into(),
            target_comp_id: "szsim01".into(),
            heart_btint: 10,
            password: String::new(),
            default_appl_ver_id: String::new(),
        })
    }

    #[test]
    fn test_roundtrip() {
        let mut buf = BytesMut::new();
        encode_frame(sample_logon(), &mut buf);
        let decoded = decode_frame(&mut buf).unwrap().expect("应有完整一帧");
        assert_eq!(decoded.msg_type, MSG_LOGON);
        assert!(matches!(decoded.body, SzseBinaryBodyEnum::Logon(_)));
        assert!(buf.is_empty(), "帧应被完整消费");
    }

    #[test]
    fn test_two_frames_in_one_buffer() {
        let mut buf = BytesMut::new();
        encode_frame(sample_logon(), &mut buf);
        encode_frame(SzseBinaryBodyEnum::Heartbeat(Heartbeat {}), &mut buf);
        assert_eq!(decode_frame(&mut buf).unwrap().unwrap().msg_type, MSG_LOGON);
        assert_eq!(
            decode_frame(&mut buf).unwrap().unwrap().msg_type,
            MSG_HEARTBEAT
        );
        assert!(decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_partial_frame_needs_more_data() {
        let mut full = BytesMut::new();
        encode_frame(sample_logon(), &mut full);
        let total = full.len();
        let mut buf = full.split_to(total / 2);
        assert!(decode_frame(&mut buf).unwrap().is_none());
        buf.unsplit(full);
        assert!(decode_frame(&mut buf).unwrap().is_some());
    }

    #[test]
    fn test_unknown_message_type_rejected() {
        let mut buf = BytesMut::new();
        encode_frame(SzseBinaryBodyEnum::Heartbeat(Heartbeat {}), &mut buf);
        buf[0..4].copy_from_slice(&999u32.to_be_bytes());
        match decode_frame(&mut buf) {
            Err(FrameError::UnknownMessageType { msg_type }) => assert_eq!(msg_type, 999),
            other => panic!("期望 UnknownMessageType，实际 {other:?}"),
        }
    }

    #[test]
    fn test_oversized_body_len_rejected() {
        let mut buf = BytesMut::new();
        encode_frame(SzseBinaryBodyEnum::Heartbeat(Heartbeat {}), &mut buf);
        buf[4..8].copy_from_slice(&(MAX_BODY_LEN + 1).to_be_bytes());
        assert!(matches!(
            decode_frame(&mut buf),
            Err(FrameError::BodyTooLarge { .. })
        ));
    }
}
