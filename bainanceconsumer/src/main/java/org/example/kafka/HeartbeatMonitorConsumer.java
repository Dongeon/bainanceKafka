package org.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * producer-heartbeat 토픽을 소비해 heartbeat 수신 여부를 로깅한다.
 *
 * <p>ACTIVE/OFFLINE 상태 갱신은 ProducerStatusConsumer가 전담한다.
 * 이 클래스는 heartbeat 흐름 모니터링 용도로만 사용한다.
 */
public class HeartbeatMonitorConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitorConsumer.class);

    private static final String TOPIC = "producer-heartbeat";

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper                  mapper  = new ObjectMapper();
    private final AtomicBoolean                 running = new AtomicBoolean(true);

    public HeartbeatMonitorConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "producer-monitor-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(TOPIC));
        log.info("HeartbeatMonitorConsumer 시작 — topic: {}", TOPIC);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logHeartbeat(record.value());
                }
            }
        } catch (WakeupException e) {
            if (running.get()) log.error("예상치 못한 WakeupException", e);
        } finally {
            consumer.close();
            log.info("HeartbeatMonitorConsumer 종료");
        }
    }

    private void logHeartbeat(String json) {
        try {
            JsonNode node       = mapper.readTree(json);
            String   instanceId = node.get("instanceId").asText();
            int      weight     = node.get("weight").asInt();
            log.debug("[HEARTBEAT] {} weight={}", instanceId, weight);
        } catch (Exception e) {
            log.error("heartbeat 파싱 실패: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
