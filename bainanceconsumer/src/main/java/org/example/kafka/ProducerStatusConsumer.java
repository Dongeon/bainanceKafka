package org.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.service.ProducerMonitoringRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * producer-status 토픽을 소비해 producer_monitoring 테이블을 갱신한다.
 *
 * <p>LeaderElector가 상태 전환 시마다 발행하는 이벤트를 수신한다:
 * PREPARING / ACTIVE / STANDBY / OFFLINE
 */
public class ProducerStatusConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProducerStatusConsumer.class);

    private static final String TOPIC = "producer-status";

    private final KafkaConsumer<String, String>  consumer;
    private final ProducerMonitoringRepository   repo;
    private final ObjectMapper                   mapper  = new ObjectMapper();
    private final AtomicBoolean                  running = new AtomicBoolean(true);

    public ProducerStatusConsumer(String bootstrapServers, ProducerMonitoringRepository repo) {
        this.repo = repo;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "producer-status-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(TOPIC));
        log.info("ProducerStatusConsumer 시작 — topic: {}", TOPIC);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    process(record.value());
                }
            }
        } catch (WakeupException e) {
            if (running.get()) log.error("예상치 못한 WakeupException", e);
        } finally {
            consumer.close();
            log.info("ProducerStatusConsumer 종료");
        }
    }

    private void process(String json) {
        try {
            JsonNode node       = mapper.readTree(json);
            String   instanceId = node.get("instanceId").asText();
            int      weight     = node.get("weight").asInt();
            String   state      = node.get("state").asText();

            repo.upsertStatus(instanceId, weight, state);
        } catch (Exception e) {
            log.error("status 이벤트 파싱 실패: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
