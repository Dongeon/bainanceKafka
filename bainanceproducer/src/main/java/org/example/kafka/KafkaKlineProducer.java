package org.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.KlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 확정 캔들(KlineEvent)을 Kafka crypto-kline 토픽으로 전송하는 프로듀서.
 *
 * <p>파티션 키: "BTCUSDT@1m" 형식으로 심볼+인터벌을 조합한다.
 * 같은 심볼+인터벌은 항상 동일 파티션으로 라우팅되어 시계열 순서가 보장된다.
 */
public class KafkaKlineProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaKlineProducer.class);
    public static final String TOPIC = "crypto-kline";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaKlineProducer(String bootstrapServers) {
        this.objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        this.producer = new KafkaProducer<>(props);
    }

    public void send(KlineEvent event) {
        String key   = event.getSymbol() + "@" + event.getInterval();
        String value = serialize(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception ex) {
                if (ex != null) {
                    log.error("Failed to send [{}]: {}", key, ex.getMessage());
                } else {
                    log.debug("Sent [{}] → partition={} offset={}",
                            key, metadata.partition(), metadata.offset());
                }
            }
        });
    }

    private String serialize(KlineEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for " + event.getSymbol(), e);
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("KafkaKlineProducer closed.");
    }
}
