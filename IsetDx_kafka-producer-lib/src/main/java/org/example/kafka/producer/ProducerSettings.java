package org.example.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

/**
 * kafka.conf 파일을 읽어 라이브러리 동작과 Kafka 클라이언트 설정을 제공한다.
 *
 * <p>설정 접두어 규칙:
 * <pre>
 *   kafka.*          → Kafka 클라이언트에 pass-through (보안/브로커/튜닝 등 모든 공식 설정 지원)
 *   kafka.partition.key.field  → 라이브러리 전용 (pass-through 제외)
 *   kafka.error.policy         → 라이브러리 전용 (pass-through 제외)
 *   kafka.error.topic          → 라이브러리 전용 (pass-through 제외)
 *   kafka.send.mode            → 라이브러리 전용 (pass-through 제외)
 * </pre>
 *
 * <p>파일 탐색 순서: 파일시스템 경로 → 클래스패스 리소스
 */
public class ProducerSettings {

    // kafka.conf에서 Kafka 클라이언트로 넘기지 않을 라이브러리 전용 키
    private static final Set<String> LIBRARY_KEYS = Set.of(
            "kafka.partition.key.field",
            "kafka.error.policy",
            "kafka.error.topic",
            "kafka.send.mode"
    );

    private final Properties raw;

    private ProducerSettings(Properties raw) {
        this.raw = raw;
    }

    /**
     * 파일 경로 또는 클래스패스에서 kafka.conf를 로드한다.
     *
     * @param path 파일시스템 경로 (절대/상대) 또는 클래스패스 리소스명
     */
    public static ProducerSettings load(String path) throws IOException {
        Properties props = new Properties();
        var filePath = Paths.get(path);
        if (Files.exists(filePath)) {
            try (InputStream in = Files.newInputStream(filePath)) {
                props.load(in);
            }
        } else {
            try (InputStream in = ProducerSettings.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) throw new IOException("kafka.conf not found: " + path);
                props.load(in);
            }
        }
        return new ProducerSettings(props);
    }

    /**
     * kafka.* 설정을 Kafka 클라이언트 Properties로 변환한다.
     * 라이브러리 전용 키는 제외하고, key/value serializer는 자동 주입한다.
     */
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        for (String key : raw.stringPropertyNames()) {
            if (key.startsWith("kafka.") && !LIBRARY_KEYS.contains(key)) {
                // "kafka." 접두어를 제거해 Kafka 공식 설정명으로 변환
                props.put(key.substring("kafka.".length()), raw.getProperty(key).trim());
            }
        }
        // JSON 문자열 전송이므로 key/value 모두 String 직렬화 고정
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    // ── 라이브러리 동작 설정 ─────────────────────────────────────────────────

    /**
     * 파티션 키로 사용할 JSON 필드명.
     * 빈 문자열이면 라운드로빈 (null key) 방식으로 동작한다.
     */
    public String partitionKeyField() {
        return raw.getProperty("kafka.partition.key.field", "").trim();
    }

    /** 전송 실패 시 정책. log_and_skip(기본) 또는 dead_letter. */
    public ErrorPolicy errorPolicy() {
        return ErrorPolicy.from(raw.getProperty("kafka.error.policy", "log_and_skip"));
    }

    /** dead_letter 정책 사용 시 실패 메시지를 전송할 토픽명. */
    public String errorTopic() {
        return raw.getProperty("kafka.error.topic", "").trim();
    }

    /** 기본 전송 모드. async(기본) 또는 sync. */
    public SendMode sendMode() {
        return SendMode.from(raw.getProperty("kafka.send.mode", "async"));
    }

    /** 설정값 직접 조회 (없으면 defaultValue 반환). */
    public String get(String key, String defaultValue) {
        return raw.getProperty(key, defaultValue).trim();
    }
}
