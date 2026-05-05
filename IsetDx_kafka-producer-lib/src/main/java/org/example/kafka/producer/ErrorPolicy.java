package org.example.kafka.producer;

/**
 * Kafka 전송 실패 시 처리 정책.
 *
 * <p>LOG_AND_SKIP:  오류 로그만 남기고 해당 메시지 폐기. 서비스는 계속 진행.
 * <p>DEAD_LETTER:   원본 메시지 + 실패 원인을 별도 error 토픽으로 전송. 재처리 가능.
 */
public enum ErrorPolicy {
    LOG_AND_SKIP,
    DEAD_LETTER;

    public static ErrorPolicy from(String value) {
        if (value == null) return LOG_AND_SKIP;
        return switch (value.trim().toLowerCase()) {
            case "dead_letter" -> DEAD_LETTER;
            default            -> LOG_AND_SKIP;
        };
    }
}
