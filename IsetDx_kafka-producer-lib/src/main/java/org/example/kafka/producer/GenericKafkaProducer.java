package org.example.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 도메인 무관 범용 Kafka 프로듀서 라이브러리.
 *
 * <p>핵심 설계 원칙:
 * <ul>
 *   <li>입력 타입 {@code Object} — 어떤 도메인 객체든 Jackson이 JSON으로 직렬화
 *   <li>라이브러리 코드 수정 없이 kafka.conf만으로 모든 동작 변경 가능
 *   <li>SASL_SSL 포함 모든 Kafka 공식 설정을 pass-through로 지원
 *   <li>다중 토픽 — send 시점에 토픽 지정
 *   <li>graceful shutdown — {@code close()} 시 미전송 메시지를 모두 flush 후 종료
 * </ul>
 *
 * <p>기본 사용 예:
 * <pre>{@code
 *   GenericKafkaProducer producer = GenericKafkaProducer.fromConfig("kafka.conf");
 *
 *   producer.send("crypto-ticker", tickerEvent);           // 비동기
 *   producer.sendSync("crypto-kline", klineEvent);         // 동기 (ack 확인)
 *
 *   producer.close();  // 종료 전 반드시 호출
 * }</pre>
 *
 * <p>이 클래스는 스레드 안전하다. 여러 스레드에서 동시에 send()를 호출해도 된다.
 */
public class GenericKafkaProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GenericKafkaProducer.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper                  mapper;
    private final PartitionKeyExtractor         keyExtractor;
    private final ErrorPolicy                   errorPolicy;
    private final String                        errorTopic;

    // ── 생성 ─────────────────────────────────────────────────────────────────

    /**
     * kafka.conf 파일 경로로부터 프로듀서를 생성한다.
     *
     * @param configPath kafka.conf 파일 경로 (파일시스템 또는 클래스패스)
     */
    public static GenericKafkaProducer fromConfig(String configPath) throws IOException {
        return new GenericKafkaProducer(ProducerSettings.load(configPath));
    }

    /**
     * 이미 로드된 {@link ProducerSettings}으로 프로듀서를 생성한다.
     * 여러 컴포넌트가 동일한 설정 파일을 공유할 때 사용한다.
     */
    public GenericKafkaProducer(ProducerSettings settings) {
        this.mapper       = new ObjectMapper();
        this.keyExtractor = new PartitionKeyExtractor(settings.partitionKeyField());
        this.errorPolicy  = settings.errorPolicy();
        this.errorTopic   = settings.errorTopic();
        this.producer     = new KafkaProducer<>(settings.toKafkaProperties());

        log.info("GenericKafkaProducer initialized [errorPolicy={}, keyField='{}', sendMode={}]",
                errorPolicy, settings.partitionKeyField(), settings.sendMode());
    }

    // ── 전송 API ─────────────────────────────────────────────────────────────

    /**
     * 지정한 토픽으로 비동기 전송한다. 즉시 리턴하며 결과는 내부적으로 처리된다.
     *
     * @param topic 전송 대상 Kafka 토픽
     * @param data  전송할 객체 (Jackson으로 JSON 직렬화)
     */
    public void send(String topic, Object data) {
        String json = serialize(topic, data);
        if (json == null) return;

        String key    = keyExtractor.extract(json);
        var    record = new ProducerRecord<>(topic, key, json);

        final String fKey   = key;
        final String fTopic = topic;
        final String fJson  = json;
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata meta, Exception ex) {
                if (ex != null) handleError(fTopic, fJson, ex);
                else log.debug("Sent → topic={} partition={} offset={} key={}",
                        meta.topic(), meta.partition(), meta.offset(), fKey);
            }
        });
    }

    /**
     * 지정한 토픽으로 동기 전송한다. 브로커 응답을 받을 때까지 블로킹된다.
     *
     * @param topic 전송 대상 Kafka 토픽
     * @param data  전송할 객체
     * @return 전송 완료된 파티션/오프셋 정보. 실패 시 null.
     */
    public RecordMetadata sendSync(String topic, Object data) {
        String json = serialize(topic, data);
        if (json == null) return null;

        String key    = keyExtractor.extract(json);
        var    record = new ProducerRecord<>(topic, key, json);

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata meta = future.get();  // 브로커 응답까지 블로킹
            log.debug("SendSync OK → topic={} partition={} offset={} key={}",
                    meta.topic(), meta.partition(), meta.offset(), key);
            return meta;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleError(topic, json, e);
            return null;
        } catch (ExecutionException e) {
            handleError(topic, json, e.getCause() != null ? (Exception) e.getCause() : e);
            return null;
        }
    }

    // ── 종료 ─────────────────────────────────────────────────────────────────

    /**
     * 내부 버퍼의 미전송 메시지를 모두 전송(flush)한 뒤 Kafka 연결을 닫는다.
     * 조건 5(graceful shutdown) 구현 — 반드시 호출해야 메시지 유실이 없다.
     */
    @Override
    public void close() {
        log.info("GenericKafkaProducer closing — flushing remaining messages...");
        producer.flush();
        producer.close();
        log.info("GenericKafkaProducer closed.");
    }

    // ── 내부 처리 ─────────────────────────────────────────────────────────────

    private String serialize(String topic, Object data) {
        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error("[{}] Serialization failed: {}", topic, e.getMessage());
            return null;
        }
    }

    private void handleError(String topic, String json, Exception ex) {
        log.error("[{}] Send failed: {}", topic, ex.getMessage());
        if (errorPolicy == ErrorPolicy.DEAD_LETTER) {
            sendToDeadLetter(topic, json, ex);
        }
        // LOG_AND_SKIP: 로그만 남기고 계속
    }

    private void sendToDeadLetter(String originalTopic, String originalJson, Exception cause) {
        if (errorTopic.isBlank()) {
            log.warn("dead_letter policy set but kafka.error.topic is empty — skipping dead letter");
            return;
        }
        try {
            Map<String, Object> dlq = new LinkedHashMap<>();
            dlq.put("originalTopic",   originalTopic);
            dlq.put("originalPayload", mapper.readTree(originalJson));
            dlq.put("errorMessage",    cause.getMessage());
            dlq.put("failedAt",        Instant.now().toString());

            String dlqJson = mapper.writeValueAsString(dlq);
            producer.send(new ProducerRecord<>(errorTopic, null, dlqJson), new Callback() {
                public void onCompletion(RecordMetadata meta, Exception ex) {
                    if (ex != null) log.error("Dead letter send failed: {}", ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Dead letter serialization failed: {}", e.getMessage());
        }
    }
}
