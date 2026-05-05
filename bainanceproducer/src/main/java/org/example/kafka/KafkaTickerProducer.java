package org.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * TickerEvent를 JSON으로 직렬화하여 Kafka 토픽으로 전송하는 프로듀서.
 *
 * <p>역할:
 *   TickerEventHandler로부터 이벤트를 받아 JSON 문자열로 직렬화한 뒤
 *   Kafka의 "crypto-ticker" 토픽으로 비동기 전송한다.
 *
 * <p>파티셔닝 전략:
 *   심볼(BTCUSDT 등)을 파티션 키로 사용한다.
 *   같은 심볼은 항상 동일한 파티션으로 라우팅되어 시계열 순서가 보장된다.
 *   → 파티션 매핑 예시: BTCUSDT → partition 8, ETHUSDT → partition 9 (murmur2 해시)
 *
 * <p>AutoCloseable 구현:
 *   try-with-resources 또는 ShutdownHook에서 close()를 호출하면
 *   버퍼에 남은 메시지를 모두 전송(flush)한 뒤 연결을 닫는다.
 */
public class KafkaTickerProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaTickerProducer.class);

    /** 전송 대상 Kafka 토픽 이름. Consumer와 반드시 동일한 값을 사용해야 한다. */
    public static final String TOPIC = "crypto-ticker";

    /** Kafka 클라이언트. Key/Value 모두 String(JSON 문자열) 타입으로 사용한다. */
    private final KafkaProducer<String, String> producer;

    /** TickerEvent → JSON 직렬화에 사용하는 Jackson ObjectMapper */
    private final ObjectMapper objectMapper;

    /**
     * 생성자. Kafka 브로커 주소를 받아 프로듀서를 초기화한다.
     *
     * @param bootstrapServers Kafka 브로커 주소 (예: "localhost:9092")
     */
    public KafkaTickerProducer(String bootstrapServers) {
        this.objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ── 처리량 vs 지연 균형 설정 ─────────────────────────────────────────────
        // acks=1: Leader 파티션의 확인만 기다린다.
        //   - acks=all 대비 속도가 빠르나, Leader 장애 시 극히 드물게 메시지 유실 가능.
        //   - 실시간 시세 데이터는 약간의 유실보다 낮은 지연이 더 중요하므로 1을 선택.
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // linger.ms=10: 메시지를 즉시 보내지 않고 최대 10ms 대기하며 배치를 채운다.
        //   - 바이낸스는 초당 수십 건씩 이벤트를 보내므로 배치 효과가 좋다.
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");

        // batch.size=16KB: 배치 최대 크기. linger.ms와 함께 배치 전송 효율을 결정한다.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        // compression.type=lz4: 네트워크 전송량과 브로커 디스크 사용량을 줄여준다.
        //   - lz4는 CPU 부담이 낮고 압축/해제 속도가 빠르다.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // enable.idempotence=false: acks=1과 idempotence는 함께 사용할 수 없다.
        //   - idempotence를 true로 바꾸려면 acks도 "all"로 변경해야 한다.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * TickerEvent를 직렬화하여 Kafka에 비동기 전송한다.
     *
     * <p>키(심볼)가 파티션 결정에 사용되므로 절대 null로 전송하면 안 된다.
     * null 키는 라운드로빈으로 파티셔닝되어 동일 심볼이 여러 파티션에 분산된다.
     *
     * @param event 전송할 시세 이벤트
     */
    public void send(TickerEvent event) {
        String key = event.getSymbol();   // 파티션 키 — 심볼별로 동일 파티션 보장
        String value = serialize(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

        // 비동기 콜백: 전송 완료(또는 실패) 시 Kafka 내부 스레드에서 호출된다.
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception ex) {
                if (ex != null) {
                    log.error("Failed to send [{}]: {}", key, ex.getMessage());
                } else {
                    log.debug("Sent [{}] → topic={} partition={} offset={}",
                            key, metadata.topic(), metadata.partition(), metadata.offset());
                }
            }
        });
    }

    /**
     * TickerEvent를 JSON 문자열로 직렬화한다.
     * getEventTimeKst()가 @JsonProperty("eventTimeKst")로 노출되어 JSON에 포함된다.
     *
     * @param event 직렬화할 이벤트
     * @return JSON 문자열
     * @throws RuntimeException 직렬화 실패 시 (정상적으로는 발생하지 않음)
     */
    private String serialize(TickerEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for " + event.getSymbol(), e);
        }
    }

    /**
     * 내부 버퍼에 남은 메시지를 모두 전송한 뒤 Kafka 연결을 닫는다.
     * Main의 ShutdownHook에서 반드시 호출해야 종료 시 메시지 유실을 방지할 수 있다.
     */
    @Override
    public void close() {
        producer.flush(); // 버퍼에 남은 메시지 강제 전송
        producer.close();
        log.info("KafkaTickerProducer closed.");
    }
}
