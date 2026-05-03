package org.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.model.TickerEvent;
import org.example.service.TickerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TickerConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TickerConsumer.class);
    private static final String TOPIC = "crypto-ticker";
    private static final int STATUS_INTERVAL = 500; // N건마다 상태 로그

    private final KafkaConsumer<String, String> consumer;
    private final TickerRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong messageCount = new AtomicLong(0);

    public TickerConsumer(String bootstrapServers, String groupId, TickerRepository repository) {
        this.repository = repository;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");    // 신규 메시지부터 소비
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");    // 수동 커밋 (DB 저장 후 커밋)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");        // 1회 poll 최대 100건

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(TOPIC));
        log.info("Consumer 시작 — topic: {}", TOPIC);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync(); // DB 저장 완료 후 offset 커밋
                }
            }
        } catch (WakeupException e) {
            if (running.get()) log.error("예상치 못한 WakeupException", e);
        } finally {
            consumer.close();
            log.info("Consumer 종료 — 총 처리 {}건", messageCount.get());
        }
    }

    private void process(ConsumerRecord<String, String> record) {
        try {
            TickerEvent event = objectMapper.readValue(record.value(), TickerEvent.class);
            repository.save(event);

            long count = messageCount.incrementAndGet();
            if (count % STATUS_INTERVAL == 0) {
                log.info("[STATUS] 누적 저장 {}건 | 마지막: {} price={}",
                        count, event.getSymbol(), event.getLastPrice());
            }
        } catch (Exception e) {
            log.error("메시지 처리 실패 [partition={} offset={}]: {}",
                    record.partition(), record.offset(), e.getMessage());
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
