package org.example.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HeartbeatMonitor + TopicMonitor를 내부적으로 관리하는 파사드.
 *
 * <p>{@code start()} 호출 시 ShutdownHook이 자동 등록되어
 * 개발자가 별도로 {@code stop()}을 호출하거나 종료 처리를 신경 쓸 필요가 없다.
 *
 * <pre>{@code
 *   KafkaMonitor.fromConfig("kafka.conf").start();
 * }</pre>
 */
public class KafkaMonitor {

    private static final Logger log = LoggerFactory.getLogger(KafkaMonitor.class);

    // ── Builder ──────────────────────────────────────────────────────────────

    public static Builder fromConfig(String configPath) throws IOException {
        return new Builder(ConsumerSettings.load(configPath));
    }

    public static Builder fromSettings(ConsumerSettings settings) {
        return new Builder(settings);
    }

    public static class Builder {
        private final ConsumerSettings settings;

        private HeartbeatMonitor.HeartbeatHandler    heartbeatHandler    = new HeartbeatMonitor.HeartbeatHandler()    { public void handle(HeartbeatEvent e) {} };
        private HeartbeatMonitor.StatusChangeHandler statusChangeHandler = new HeartbeatMonitor.StatusChangeHandler() { public void handle(StatusEvent e) {} };
        private HeartbeatMonitor.TimeoutHandler      timeoutHandler      = new HeartbeatMonitor.TimeoutHandler()      { public void handle(String id, long ms) {} };

        private Builder(ConsumerSettings settings) {
            this.settings = settings;
        }

        public Builder onHeartbeat(HeartbeatMonitor.HeartbeatHandler handler) {
            this.heartbeatHandler = handler;
            return this;
        }

        public Builder onStatusChange(HeartbeatMonitor.StatusChangeHandler handler) {
            this.statusChangeHandler = handler;
            return this;
        }

        public Builder onTimeout(HeartbeatMonitor.TimeoutHandler handler) {
            this.timeoutHandler = handler;
            return this;
        }

        public KafkaMonitor build() {
            HeartbeatMonitor hb = HeartbeatMonitor.fromSettings(settings)
                    .onHeartbeat(heartbeatHandler)
                    .onStatusChange(statusChangeHandler)
                    .onTimeout(timeoutHandler)
                    .build();
            TopicMonitor tm = TopicMonitor.fromSettings(settings).build();
            return new KafkaMonitor(hb, tm);
        }

        public void start() {
            build().start();
        }
    }

    // ── 필드 ─────────────────────────────────────────────────────────────────

    private final HeartbeatMonitor heartbeatMonitor;
    private final TopicMonitor     topicMonitor;

    private KafkaMonitor(HeartbeatMonitor heartbeatMonitor, TopicMonitor topicMonitor) {
        this.heartbeatMonitor = heartbeatMonitor;
        this.topicMonitor     = topicMonitor;
    }

    // ── 공개 API ─────────────────────────────────────────────────────────────

    public void start() {
        heartbeatMonitor.start();
        topicMonitor.start();

        final KafkaMonitor self = this;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                self.stop();
            }
        }, "kafka-monitor-shutdown"));

        log.info("KafkaMonitor 시작");
    }

    public void stop() {
        heartbeatMonitor.stop();
        topicMonitor.stop();
        log.info("KafkaMonitor 종료");
    }
}
