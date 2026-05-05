package org.example.kafka.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * heartbeat 토픽을 구독하여 다른 인스턴스의 생존 신호를 감지한다.
 *
 * <p>설계 포인트:
 * <ul>
 *   <li>group.id = "heartbeat-watcher-{instanceId}" — 인스턴스마다 독립된 그룹.
 *       같은 토픽을 모든 인스턴스가 각자 전부 수신한다 (브로드캐스트 효과).
 *   <li>자신의 instanceId와 동일한 메시지는 무시한다.
 *   <li>메시지 파싱/처리는 onForeignHeartbeat 콜백에 위임한다.
 *       이 클래스는 수신 전담, 판단은 LeaderElector가 담당한다.
 * </ul>
 */
class HeartbeatWatcher {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatWatcher.class);

    private final KafkaConsumer<String, String>  consumer;
    private final ObjectMapper                   mapper = new ObjectMapper();
    private final String                         instanceId;
    private final String                         topic;
    private final Consumer<HeartbeatMessage>     onForeignHeartbeat;

    private volatile boolean running = false;
    private Thread watcherThread;

    HeartbeatWatcher(LeaderSettings settings, Consumer<HeartbeatMessage> onForeignHeartbeat) {
        this.instanceId         = settings.instanceId();
        this.topic              = settings.heartbeatTopic();
        this.onForeignHeartbeat = onForeignHeartbeat;
        this.consumer           = new KafkaConsumer<>(settings.toConsumerProperties());
    }

    /** heartbeat 수신 루프를 데몬 스레드로 시작한다. */
    void start() {
        running       = true;
        watcherThread = new Thread(this::watch, "heartbeat-watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        log.info("[{}] HeartbeatWatcher started (topic={})", instanceId, topic);
    }

    /** poll 루프를 중단한다. wakeup()으로 즉시 WakeupException을 발생시킨다. */
    void stop() {
        running = false;
        consumer.wakeup();
    }

    private void watch() {
        try {
            consumer.subscribe(List.of(topic));
            while (running) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        HeartbeatMessage msg = mapper.readValue(record.value(), HeartbeatMessage.class);
                        if (!instanceId.equals(msg.getInstanceId())) {
                            log.debug("[{}] Foreign heartbeat ← {} (weight={})",
                                    instanceId, msg.getInstanceId(), msg.getWeight());
                            onForeignHeartbeat.accept(msg);
                        }
                    } catch (Exception e) {
                        log.error("[{}] Failed to parse heartbeat: {}", instanceId, e.getMessage());
                    }
                }
            }
        } catch (WakeupException ignored) {
            // stop() 호출 시 정상 종료 경로
        } finally {
            consumer.close();
            log.info("[{}] HeartbeatWatcher stopped", instanceId);
        }
    }
}
