package org.example.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * producer-heartbeat 토픽을 구독하여 다른 인스턴스의 생존 신호를 감지한다.
 *
 * <p>수신한 heartbeat를 LeaderElector에 전달(onForeignHeartbeat 콜백)하여
 * 상태 전환 여부를 결정하게 한다.
 * 타임아웃 판단은 LeaderElector가 담당하고, 이 클래스는 메시지 수신만 책임진다.
 */
public class HeartbeatWatcher {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatWatcher.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String instanceId;
    private final Consumer<HeartbeatMessage> onForeignHeartbeat;

    private volatile boolean running = false;
    private Thread watcherThread;

    public HeartbeatWatcher(String bootstrapServers, String instanceId,
                            Consumer<HeartbeatMessage> onForeignHeartbeat) {
        this.instanceId          = instanceId;
        this.onForeignHeartbeat  = onForeignHeartbeat;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // 인스턴스마다 독립된 group.id → 모든 인스턴스가 동일한 heartbeat 메시지를 각자 수신
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "heartbeat-watcher-" + instanceId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        running = true;
        watcherThread = new Thread(this::watch, "heartbeat-watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        log.info("[{}] HeartbeatWatcher started", instanceId);
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    private void watch() {
        try {
            consumer.subscribe(List.of(HeartbeatPublisher.TOPIC));
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        HeartbeatMessage msg = mapper.readValue(record.value(), HeartbeatMessage.class);
                        if (!instanceId.equals(msg.getInstanceId())) {
                            log.debug("[{}] Foreign heartbeat from {}", instanceId, msg.getInstanceId());
                            onForeignHeartbeat.accept(msg);
                        }
                    } catch (Exception e) {
                        log.error("[{}] Failed to parse heartbeat: {}", instanceId, e.getMessage());
                    }
                }
            }
        } catch (WakeupException ignored) {
            // stop() 호출 시 정상 종료
        } finally {
            consumer.close();
            log.info("[{}] HeartbeatWatcher stopped", instanceId);
        }
    }
}
