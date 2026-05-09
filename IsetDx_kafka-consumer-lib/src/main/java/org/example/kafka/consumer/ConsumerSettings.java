package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * kafka.conf 파일에서 HeartbeatMonitor에 필요한 설정을 읽는다.
 *
 * <p>설정 접두어 규칙:
 * <pre>
 *   kafka.*      → Kafka 브로커 공통 설정 (보안/연결 등) — LeaderSettings 와 동일 파일 공유 가능
 *   consumer.*   → HeartbeatMonitor 전용 (토픽명, timeout, 집계 주기)
 *   db.*         → DB 연결 정보 (url, user, password)
 * </pre>
 */
public class ConsumerSettings {

    private final Properties raw;

    private ConsumerSettings(Properties raw) {
        this.raw = raw;
    }

    public static ConsumerSettings fromProperties(Properties props) {
        return new ConsumerSettings(props);
    }

    /** 파일시스템 경로 또는 클래스패스에서 kafka.conf를 로드한다. */
    public static ConsumerSettings load(String path) throws IOException {
        Properties props = new Properties();
        var filePath = Paths.get(path);
        if (Files.exists(filePath)) {
            try (InputStream in = Files.newInputStream(filePath)) {
                props.load(in);
            }
        } else {
            try (InputStream in = ConsumerSettings.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) throw new IOException("kafka.conf not found: " + path);
                props.load(in);
            }
        }
        return new ConsumerSettings(props);
    }

    // ── Consumer 설정 ────────────────────────────────────────────────────────

    public String heartbeatTopic() {
        return raw.getProperty("consumer.heartbeat.topic", "producer-heartbeat").trim();
    }

    public String statusTopic() {
        return raw.getProperty("consumer.status.topic", "producer-status").trim();
    }

    /** heartbeat 수신이 이 시간(ms) 이상 없으면 onTimeout 콜백 호출. 기본값 15000. */
    public long heartbeatTimeoutMs() {
        return getLong("consumer.heartbeat.timeout.ms", 15_000);
    }

    /** topic_throughput_stats 집계 주기 (ms). 기본값 60000. */
    public long throughputIntervalMs() {
        return getLong("consumer.throughput.interval.ms", 60_000);
    }

    // ── TopicMonitor 설정 ────────────────────────────────────────────────────

    /** 모니터링에서 제외할 토픽 prefix. 기본값 "__" (Kafka 내부 토픽). */
    public String topicExcludePrefix() {
        return raw.getProperty("monitor.topic.exclude.prefix", "__").trim();
    }

    /** AdminClient로 토픽 offset/lag 을 조회하는 주기 (ms). 기본값 60000. */
    public long topicMonitorIntervalMs() {
        return getLong("monitor.topic.interval.ms", 60_000);
    }

    // ── DB 설정 ──────────────────────────────────────────────────────────────

    public String dbUrl() {
        return raw.getProperty("db.url", "").trim();
    }

    public String dbUser() {
        return raw.getProperty("db.user", "").trim();
    }

    public String dbPassword() {
        return raw.getProperty("db.password", "").trim();
    }

    // ── Kafka Consumer Properties 생성 ───────────────────────────────────────

    /**
     * KafkaConsumer 생성용 Properties.
     * kafka.* 공통 설정을 pass-through하고 groupId와 String deserializer를 주입한다.
     */
    public Properties toConsumerProperties(String groupId) {
        Properties props = commonKafkaProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                  groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,     "10000");
        return props;
    }

    /** AdminClient 생성용 Properties. kafka.* 공통 설정을 pass-through한다. */
    public Properties toAdminClientProperties() {
        return commonKafkaProperties();
    }

    private Properties commonKafkaProperties() {
        Properties props = new Properties();
        for (String key : raw.stringPropertyNames()) {
            if (key.startsWith("kafka.")) {
                props.put(key.substring("kafka.".length()), raw.getProperty(key).trim());
            }
        }
        return props;
    }

    private long getLong(String key, long defaultVal) {
        String val = raw.getProperty(key);
        return val != null ? Long.parseLong(val.trim()) : defaultVal;
    }
}
