package org.example.kafka.leader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

/**
 * kafka.conf 파일에서 리더 선출에 필요한 설정을 읽는다.
 *
 * <p>설정 접두어 규칙:
 * <pre>
 *   kafka.*   → Kafka 브로커 공통 설정 (보안/연결 등) — producer/consumer 모두 pass-through
 *   leader.*  → 리더 선출 전용 설정
 * </pre>
 *
 * <p>kafka-producer-lib의 ProducerSettings와 동일한 kafka.conf 파일을 공유한다.
 */
public class LeaderSettings {

    private final Properties raw;

    private LeaderSettings(Properties raw) {
        this.raw = raw;
    }

    /** 이미 로드된 Properties로 LeaderSettings를 생성한다 (CLI 오버라이드 등). */
    public static LeaderSettings fromProperties(Properties props) {
        return new LeaderSettings(props);
    }

    /** 파일시스템 경로 또는 클래스패스에서 kafka.conf를 로드한다. */
    public static LeaderSettings load(String path) throws IOException {
        Properties props = new Properties();
        var filePath = Paths.get(path);
        if (Files.exists(filePath)) {
            try (InputStream in = Files.newInputStream(filePath)) {
                props.load(in);
            }
        } else {
            try (InputStream in = LeaderSettings.class.getClassLoader().getResourceAsStream(path)) {
                if (in == null) throw new IOException("kafka.conf not found: " + path);
                props.load(in);
            }
        }
        return new LeaderSettings(props);
    }

    // ── 리더 선출 설정 ───────────────────────────────────────────────────────

    /** 이 인스턴스 고유 식별자. 미설정 시 랜덤 생성. */
    public String instanceId() {
        String val = raw.getProperty("leader.instance.id", "").trim();
        return val.isBlank()
                ? "leader-" + UUID.randomUUID().toString().substring(0, 6)
                : val;
    }

    /**
     * 이 인스턴스의 우선순위 가중치.
     * 값이 클수록 split-brain 발생 시 ACTIVE를 유지할 확률이 높다.
     * 기본값 100.
     */
    public int instanceWeight() {
        return getInt("leader.instance.weight", 100);
    }

    /** heartbeat 메시지를 주고받을 Kafka 토픽. 기본값 "producer-heartbeat". */
    public String heartbeatTopic() {
        return raw.getProperty("leader.heartbeat.topic", "producer-heartbeat").trim();
    }

    /** PREPARING 구간 기본 대기 시간 (ms). 기본값 5000. */
    public long prepBaseMs() {
        return getLong("leader.prep.base.ms", 5_000);
    }

    /** PREPARING 구간에 추가되는 랜덤 지터 최대값 (ms). 기본값 2000. */
    public long prepJitterMs() {
        return getLong("leader.prep.jitter.ms", 2_000);
    }

    /** STANDBY 상태에서 이 시간(ms) 동안 heartbeat가 없으면 ACTIVE로 전환. 기본값 15000. */
    public long activationTimeoutMs() {
        return getLong("leader.activation.timeout.ms", 15_000);
    }

    /** ACTIVE 상태에서 heartbeat를 전송하는 주기 (초). 기본값 5. */
    public long heartbeatIntervalSec() {
        return getLong("leader.heartbeat.interval.sec", 5);
    }

    /** 상태 전환 이벤트를 발행할 Kafka 토픽. 기본값 "producer-status". */
    public String statusEventTopic() {
        return raw.getProperty("leader.status.topic", "producer-status").trim();
    }

    // ── Kafka 클라이언트 Properties 생성 ────────────────────────────────────

    /**
     * HeartbeatPublisher(Kafka Producer)용 Properties.
     * kafka.* 공통 설정을 pass-through하고 String serializer를 주입한다.
     */
    public Properties toProducerProperties() {
        Properties props = commonKafkaProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,      "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");   // heartbeat는 즉시 전송
        return props;
    }

    /**
     * HeartbeatWatcher(Kafka Consumer)용 Properties.
     * 인스턴스마다 독립적인 group.id를 사용해 모든 인스턴스가 동일한 메시지를 수신한다.
     */
    public Properties toConsumerProperties() {
        Properties props = commonKafkaProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 인스턴스별 독립 group → 같은 heartbeat를 모든 인스턴스가 각자 수신
        props.put(ConsumerConfig.GROUP_ID_CONFIG,              "heartbeat-watcher-" + instanceId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,     "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,    "true");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,  "10000");
        return props;
    }

    /** kafka.* 접두어 설정을 Kafka 공식 키로 변환 (접두어 제거). */
    private Properties commonKafkaProperties() {
        Properties props = new Properties();
        for (String key : raw.stringPropertyNames()) {
            if (key.startsWith("kafka.")) {
                props.put(key.substring("kafka.".length()), raw.getProperty(key).trim());
            }
        }
        return props;
    }

    private int getInt(String key, int defaultVal) {
        String val = raw.getProperty(key);
        return val != null ? Integer.parseInt(val.trim()) : defaultVal;
    }

    private long getLong(String key, long defaultVal) {
        String val = raw.getProperty(key);
        return val != null ? Long.parseLong(val.trim()) : defaultVal;
    }
}
