package org.example.kafka.producer;

/**
 * Kafka 전송 모드.
 *
 * <p>ASYNC: 전송 요청 후 즉시 리턴. 결과는 내부 콜백으로 처리.
 * <p>SYNC:  브로커 응답 수신까지 블로킹. RecordMetadata 반환.
 */
public enum SendMode {
    ASYNC,
    SYNC;

    public static SendMode from(String value) {
        if (value == null) return ASYNC;
        return switch (value.trim().toLowerCase()) {
            case "sync" -> SYNC;
            default     -> ASYNC;
        };
    }
}
