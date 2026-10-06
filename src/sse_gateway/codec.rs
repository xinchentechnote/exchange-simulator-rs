//! SSE 二进制帧编解码。
//!
//! 帧格式（大端序）：`msg_type(u32) + msg_seq_num(u64) + msg_body_len(u32) + body + checksum(u32)`，
//! 固定头 16 字节，校验和 4 字节；校验和由 binary_codec 全局注册的
//! `SsebinChecksumService`（字节累加 mod 256）在 encode 时自动计算，decode 不校验（宽容模式）。

use binary_codec::BinaryCodec;
use bytes::BytesMut;
use sse_binary::sse_binary::{SseBinary, SseBinaryBodyEnum};

pub const HEADER_LEN: usize = 16;
pub const CHECKSUM_LEN: usize = 4;
/// 单帧 body 上限防护，超过直接断开连接
pub const MAX_BODY_LEN: u32 = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum FrameError {
    UnknownMessageType { msg_type: u32 },
    BodyTooLarge { body_len: u32 },
    MalformedBody,
}

/// 报文类型 → 类型码（encode 需要显式回填 msg_type 字段）
pub fn msg_type_of(body: &SseBinaryBodyEnum) -> u32 {
    match body {
        SseBinaryBodyEnum::Heartbeat(_) => 33,
        SseBinaryBodyEnum::Logon(_) => 40,
        SseBinaryBodyEnum::Logout(_) => 41,
        SseBinaryBodyEnum::NewOrderSingle(_) => 58,
        SseBinaryBodyEnum::OrderCancel(_) => 61,
        SseBinaryBodyEnum::Confirm(_) => 32,
        SseBinaryBodyEnum::CancelReject(_) => 59,
        SseBinaryBodyEnum::Report(_) => 103,
        SseBinaryBodyEnum::OrderReject(_) => 204,
        SseBinaryBodyEnum::PlatformState(_) => 209,
        SseBinaryBodyEnum::ExecRptInfo(_) => 208,
        SseBinaryBodyEnum::ExecRptSync(_) => 206,
        SseBinaryBodyEnum::ExecRptSyncRsp(_) => 207,
        SseBinaryBodyEnum::ExecRptEndOfStream(_) => 210,
    }
}

/// 从缓冲区尝试提取一帧。缓冲不足返回 `Ok(None)`；
/// 未知类型/坏报文返回 `Err`（调用方决定跳过或断开）。
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<SseBinary>, FrameError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    // 布局：msg_type(0..4) + msg_seq_num(4..12) + msg_body_len(12..16)
    let body_len = u32::from_be_bytes(buf[12..HEADER_LEN].try_into().expect("切片长度恒为 4"));
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
        SseBinary::decode(&mut frame)
    }));
    match decoded {
        Ok(Some(msg)) => Ok(Some(msg)),
        Ok(None) => Err(FrameError::UnknownMessageType { msg_type }),
        Err(_) => Err(FrameError::MalformedBody),
    }
}

/// 编码一帧（校验和由 binary_codec 全局服务自动计算）。
pub fn encode_frame(body: SseBinaryBodyEnum, msg_seq_num: u64, out: &mut BytesMut) {
    let msg_type = msg_type_of(&body);
    let msg = SseBinary {
        msg_type,
        msg_seq_num,
        msg_body_len: 0,
        body,
        checksum: 0,
    };
    msg.encode(out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sse_binary::heartbeat::Heartbeat;
    use sse_binary::logon::Logon;

    fn sample_logon() -> SseBinaryBodyEnum {
        SseBinaryBodyEnum::Logon(Logon {
            sender_comp_id: "gt001".into(),
            target_comp_id: "sim001".into(),
            heart_bt_int: 10,
            prtcl_version: "v1".into(),
            trade_date: 20260501,
            q_size: 2,
        })
    }

    #[test]
    fn test_roundtrip() {
        let mut buf = BytesMut::new();
        encode_frame(sample_logon(), 7, &mut buf);
        let decoded = decode_frame(&mut buf).unwrap().expect("应有完整一帧");
        assert_eq!(decoded.msg_type, 40);
        assert_eq!(decoded.msg_seq_num, 7);
        assert!(matches!(decoded.body, SseBinaryBodyEnum::Logon(_)));
        assert!(buf.is_empty(), "帧应被完整消费");
    }

    #[test]
    fn test_two_frames_in_one_buffer() {
        let mut buf = BytesMut::new();
        encode_frame(sample_logon(), 1, &mut buf);
        encode_frame(SseBinaryBodyEnum::Heartbeat(Heartbeat {}), 2, &mut buf);
        let first = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(first.msg_type, 40);
        let second = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(second.msg_type, 33);
        assert!(decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_partial_frame_needs_more_data() {
        let mut full = BytesMut::new();
        encode_frame(sample_logon(), 1, &mut full);
        let total = full.len();
        let mut buf = full.split_to(total / 2);
        assert!(decode_frame(&mut buf).unwrap().is_none());
        buf.unsplit(full);
        assert!(decode_frame(&mut buf).unwrap().is_some());
    }

    #[test]
    fn test_unknown_message_type_rejected() {
        // 构造合法帧后篡改 msg_type 为未定义值
        let mut buf = BytesMut::new();
        encode_frame(SseBinaryBodyEnum::Heartbeat(Heartbeat {}), 1, &mut buf);
        buf[0..4].copy_from_slice(&999u32.to_be_bytes());
        match decode_frame(&mut buf) {
            Err(FrameError::UnknownMessageType { msg_type }) => assert_eq!(msg_type, 999),
            other => panic!("期望 UnknownMessageType，实际 {other:?}"),
        }
    }

    #[test]
    fn test_oversized_body_len_rejected() {
        let mut buf = BytesMut::new();
        encode_frame(SseBinaryBodyEnum::Heartbeat(Heartbeat {}), 1, &mut buf);
        buf[12..16].copy_from_slice(&(MAX_BODY_LEN + 1).to_be_bytes());
        assert!(matches!(
            decode_frame(&mut buf),
            Err(FrameError::BodyTooLarge { .. })
        ));
    }

    #[test]
    fn test_truncated_body_does_not_panic() {
        // 声明的 body_len 大于实际写入的报文体布局 → 解码应报坏报文而非 panic
        let mut buf = BytesMut::new();
        encode_frame(sample_logon(), 1, &mut buf);
        let declared = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        // 截掉一半 body 后补上校验和位置，使长度前移帧完整但内容残缺
        let mut forged = BytesMut::new();
        forged.extend_from_slice(&buf[..HEADER_LEN]);
        forged.extend_from_slice(&buf[HEADER_LEN..HEADER_LEN + declared as usize / 2]);
        forged.extend_from_slice(&[0u8; CHECKSUM_LEN]);
        // 保持 msg_body_len 与实际帧长度自洽
        let actual_body = forged.len() - HEADER_LEN - CHECKSUM_LEN;
        forged[12..16].copy_from_slice(&(actual_body as u32).to_be_bytes());
        let _ = decode_frame(&mut forged);
    }
}
