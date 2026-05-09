package org.example.kafka.leader;

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
 * producer-status 토픽을 감시해 외부 인스턴스의 OFFLINE 이벤트를 감지한다.
 *
 * <p>STANDBY 상태에서 activationTimeout(기본 15s)을 기다리지 않고
 * OFFLINE 이벤트 수신 즉시 onOffline 콜백을 호출해 빠른 페일오버를 지원한다.
 */
class StatusEventWatcher {

    private static final Logger log = LoggerFactory.getLogger(StatusEventWatcher.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper                  mapper     = new ObjectMapper();
    private final AtomicBoolean                 running    = new AtomicBoolean(true);
    private final String                        instanceId;
    private final String                        topic;
    private final Runnable                      onOffline;

    private Thread thread;

    StatusEventWatcher(LeaderSettings settings, Runnable onOffline) {
        this.instanceId = settings.instanceId();
        this.topic      = settings.statusEventTopic();
        this.onOffline  = onOffline;

        Properties props = settings.toConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG,            "status-watcher-" + instanceId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
    }

    void start() {
        final StatusEventWatcher self = this;
        thread = new Thread(new Runnable() {
            public void run() {
                consumer.subscribe(List.of(self.topic));
                log.info("[{}] StatusEventWatcher started (topic={})", self.instanceId, self.topic);
                try {
                    while (self.running.get()) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> record : records) {
                            self.process(record.value());
                        }
                    }
                } catch (WakeupException e) {
                    if (self.running.get()) log.error("[{}] StatusEventWatcher 예상치 못한 wakeup", self.instanceId, e);
                } finally {
                    consumer.close();
                    log.info("[{}] StatusEventWatcher stopped", self.instanceId);
                }
            }
        }, "status-event-watcher");
        thread.setDaemon(true);
        thread.start();
    }

    private void process(String json) {
        try {
            JsonNode node      = mapper.readTree(json);
            String   foreignId = node.get("instanceId").asText();
            String   state     = node.get("state").asText();

            if (!foreignId.equals(instanceId) && "OFFLINE".equals(state)) {
                log.info("[{}] Foreign {} → OFFLINE 감지", instanceId, foreignId);
                onOffline.run();
            }
        } catch (Exception e) {
            log.error("[{}] StatusEvent 파싱 오류: {}", instanceId, e.getMessage());
        }
    }

    void stop() {
        running.set(false);
        consumer.wakeup();
    }
}
