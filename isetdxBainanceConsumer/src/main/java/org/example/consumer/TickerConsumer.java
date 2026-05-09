package org.example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.config.AppConfig;
import org.example.model.TickerEvent;
import org.example.repository.TickerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TickerConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TickerConsumer.class);
    private static final int STATUS_INTERVAL = 500;

    private final KafkaConsumer<String, String> consumer;
    private final TickerRepository repository;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running      = new AtomicBoolean(true);
    private final AtomicLong    messageCount = new AtomicLong(0);

    public TickerConsumer(AppConfig config, TickerRepository repository) {
        this.repository = repository;
        this.topic      = config.tickerTopic();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                config.tickerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,       "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        "100");

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        log.info("Ticker consumer 시작 — topic: {}", topic);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) log.error("예상치 못한 WakeupException", e);
        } finally {
            consumer.close();
            log.info("Ticker consumer 종료 — 총 처리 {}건", messageCount.get());
        }
    }

    private void process(ConsumerRecord<String, String> record) {
        try {
            TickerEvent event = objectMapper.readValue(record.value(), TickerEvent.class);
            repository.save(event);

            long count = messageCount.incrementAndGet();
            if (count % STATUS_INTERVAL == 0) {
                log.info("[TICKER] 누적 {}건 | {} price={}", count, event.getSymbol(), event.getLastPrice());
            }
        } catch (Exception e) {
            log.error("Ticker 처리 실패 [partition={} offset={}]: {}",
                    record.partition(), record.offset(), e.getMessage());
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
