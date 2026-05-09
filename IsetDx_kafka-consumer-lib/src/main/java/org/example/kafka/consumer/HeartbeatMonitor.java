package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * producer-heartbeat / producer-status 두 토픽을 동시에 구독해
 * heartbeat 수신, 상태 전환, timeout, 처리량을 모니터링하고 DB에 적재한다.
 *
 * <p>사용 예:
 * <pre>{@code
 *   HeartbeatMonitor monitor = HeartbeatMonitor.fromConfig("kafka.conf")
 *       .onHeartbeat(new HeartbeatMonitor.HeartbeatHandler() {
 *           public void handle(HeartbeatEvent e) { ... }
 *       })
 *       .onStatusChange(new HeartbeatMonitor.StatusChangeHandler() {
 *           public void handle(StatusEvent e) { ... }
 *       })
 *       .onTimeout(new HeartbeatMonitor.TimeoutHandler() {
 *           public void handle(String instanceId, long silenceMs) { ... }
 *       })
 *       .build();
 *
 *   monitor.start();
 *   // ...
 *   monitor.stop();
 * }</pre>
 */
public class HeartbeatMonitor {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitor.class);

    // ── Handler interfaces ───────────────────────────────────────────────────

    public interface HeartbeatHandler {
        void handle(HeartbeatEvent event);
    }

    public interface StatusChangeHandler {
        void handle(StatusEvent event);
    }

    public interface TimeoutHandler {
        void handle(String instanceId, long silenceMs);
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static Builder fromConfig(String configPath) throws IOException {
        return new Builder(ConsumerSettings.load(configPath));
    }

    public static Builder fromSettings(ConsumerSettings settings) {
        return new Builder(settings);
    }

    public static class Builder {
        private final ConsumerSettings settings;
        private HeartbeatHandler    heartbeatHandler    = new HeartbeatHandler()    { public void handle(HeartbeatEvent e) {} };
        private StatusChangeHandler statusChangeHandler = new StatusChangeHandler() { public void handle(StatusEvent e) {} };
        private TimeoutHandler      timeoutHandler      = new TimeoutHandler()      { public void handle(String id, long ms) {} };

        private Builder(ConsumerSettings settings) {
            this.settings = settings;
        }

        public Builder onHeartbeat(HeartbeatHandler handler) {
            this.heartbeatHandler = handler;
            return this;
        }

        public Builder onStatusChange(StatusChangeHandler handler) {
            this.statusChangeHandler = handler;
            return this;
        }

        public Builder onTimeout(TimeoutHandler handler) {
            this.timeoutHandler = handler;
            return this;
        }

        public HeartbeatMonitor build() {
            return new HeartbeatMonitor(this);
        }
    }

    // ── 필드 ─────────────────────────────────────────────────────────────────

    private final ConsumerSettings    settings;
    private final HeartbeatHandler    heartbeatHandler;
    private final StatusChangeHandler statusChangeHandler;
    private final TimeoutHandler      timeoutHandler;
    private final HeartbeatRepository repository;

    // 인스턴스별 마지막 heartbeat 수신 시각 (ms)
    private final Map<String, Long>   lastHeartbeatMs  = new ConcurrentHashMap<>();
    // 인스턴스별 마지막 알려진 상태
    private final Map<String, String> instanceState    = new ConcurrentHashMap<>();
    // ACTIVE→STANDBY 전환 시각 (recovery_ms 계산용)
    private final Map<String, Long>   failoverOutTime  = new ConcurrentHashMap<>();

    // 처리량 집계 카운터
    private final AtomicLong heartbeatCount    = new AtomicLong(0);
    private final AtomicLong statusCount       = new AtomicLong(0);
    private volatile long    windowStart       = System.currentTimeMillis();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, String> heartbeatConsumer;
    private KafkaConsumer<String, String> statusConsumer;
    private Thread                        heartbeatThread;
    private Thread                        statusThread;
    private ScheduledExecutorService      scheduler;

    private HeartbeatMonitor(Builder builder) {
        this.settings            = builder.settings;
        this.heartbeatHandler    = builder.heartbeatHandler;
        this.statusChangeHandler = builder.statusChangeHandler;
        this.timeoutHandler      = builder.timeoutHandler;
        this.repository          = new HeartbeatRepository(builder.settings);
    }

    // ── 공개 API ─────────────────────────────────────────────────────────────

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("HeartbeatMonitor already running");
            return;
        }

        initStateFromDb();

        heartbeatConsumer = new KafkaConsumer<>(settings.toConsumerProperties("hb-monitor-consumer"));
        statusConsumer    = new KafkaConsumer<>(settings.toConsumerProperties("status-monitor-consumer"));

        final HeartbeatMonitor self = this;

        heartbeatThread = new Thread(new Runnable() {
            public void run() {
                heartbeatConsumer.subscribe(List.of(settings.heartbeatTopic()));
                log.info("heartbeat consumer 시작: topic={}", settings.heartbeatTopic());
                try {
                    while (running.get()) {
                        ConsumerRecords<String, String> records = heartbeatConsumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> r : records) {
                            self.processHeartbeat(r.value());
                        }
                    }
                } catch (WakeupException e) {
                    if (running.get()) log.error("heartbeat consumer 예상치 못한 wakeup", e);
                } finally {
                    heartbeatConsumer.close();
                    log.info("heartbeat consumer 종료");
                }
            }
        }, "hb-consumer");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();

        statusThread = new Thread(new Runnable() {
            public void run() {
                statusConsumer.subscribe(List.of(settings.statusTopic()));
                log.info("status consumer 시작: topic={}", settings.statusTopic());
                try {
                    while (running.get()) {
                        ConsumerRecords<String, String> records = statusConsumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<String, String> r : records) {
                            self.processStatus(r.value());
                        }
                    }
                } catch (WakeupException e) {
                    if (running.get()) log.error("status consumer 예상치 못한 wakeup", e);
                } finally {
                    statusConsumer.close();
                    log.info("status consumer 종료");
                }
            }
        }, "status-consumer");
        statusThread.setDaemon(true);
        statusThread.start();

        scheduler = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "hb-scheduler");
                t.setDaemon(true);
                return t;
            }
        });

        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() { self.checkTimeouts(); }
        }, 1, 1, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() { self.flushThroughput(); }
        }, settings.throughputIntervalMs(), settings.throughputIntervalMs(), TimeUnit.MILLISECONDS);

        log.info("HeartbeatMonitor 시작 (heartbeatTimeout={}ms, throughputInterval={}ms)",
                settings.heartbeatTimeoutMs(), settings.throughputIntervalMs());
    }

    public void stop() {
        running.set(false);
        if (heartbeatConsumer != null) heartbeatConsumer.wakeup();
        if (statusConsumer    != null) statusConsumer.wakeup();
        if (scheduler         != null) scheduler.shutdownNow();
        repository.close();
        log.info("HeartbeatMonitor 종료");
    }

    // ── DB 상태 초기화 ───────────────────────────────────────────────────────

    private void initStateFromDb() {
        Map<String, String> lastStates = repository.loadLastStates();
        instanceState.putAll(lastStates);

        Map<String, Long> outTimes = repository.loadFailoverOutTimes();
        failoverOutTime.putAll(outTimes);

        log.info("DB 상태 초기화 완료 — instanceState: {}, failoverOutTime: {}건",
                instanceState, failoverOutTime.size());
    }

    // ── heartbeat 처리 ───────────────────────────────────────────────────────

    private void processHeartbeat(String json) {
        try {
            HeartbeatEvent event = HeartbeatEvent.fromJson(json);
            long now     = System.currentTimeMillis();
            Long prev    = lastHeartbeatMs.put(event.getInstanceId(), now);
            long interval = (prev != null) ? (now - prev) : -1;

            heartbeatCount.incrementAndGet();
            String currentState = instanceState.get(event.getInstanceId());
            repository.upsertRealtime(event, interval, currentState);
            heartbeatHandler.handle(event);

            log.debug("[HB] {} weight={} interval={}ms", event.getInstanceId(), event.getWeight(), interval);
        } catch (Exception e) {
            log.error("heartbeat 파싱 실패: {}", e.getMessage());
            repository.insertError(settings.heartbeatTopic(), "hb-monitor-consumer",
                    "PARSE_ERROR", e.getMessage(), json);
        }
    }

    // ── status 처리 ──────────────────────────────────────────────────────────

    private void processStatus(String json) {
        try {
            StatusEvent event      = StatusEvent.fromJson(json);
            String      instanceId = event.getInstanceId();
            String      newState   = event.getState();
            String      prevState  = instanceState.put(instanceId, newState);

            statusCount.incrementAndGet();
            repository.insertStatusLog(event);
            repository.insertHistory(instanceId, event.getWeight(), newState);
            repository.updateRealtimeState(instanceId, newState);
            detectFailover(instanceId, prevState, newState);
            statusChangeHandler.handle(event);

            log.info("[STATUS] {} {} → {}", instanceId, prevState == null ? "-" : prevState, newState);
        } catch (Exception e) {
            log.error("status 파싱 실패: {}", e.getMessage());
            repository.insertError(settings.statusTopic(), "status-monitor-consumer",
                    "PARSE_ERROR", e.getMessage(), json);
        }
    }

    private void detectFailover(String instanceId, String prevState, String newState) {
        if (prevState == null) return;

        if ("ACTIVE".equals(prevState) && "STANDBY".equals(newState)) {
            failoverOutTime.put(instanceId, System.currentTimeMillis());
            repository.insertFailover(instanceId, prevState, newState, null, -1);
            log.warn("[FAILOVER] {} ACTIVE → STANDBY", instanceId);

        } else if ("STANDBY".equals(prevState) && "ACTIVE".equals(newState)) {
            Long outTime     = failoverOutTime.remove(instanceId);
            long recoveryMs  = (outTime != null) ? (System.currentTimeMillis() - outTime) : -1;
            repository.insertFailover(instanceId, prevState, newState, "RECOVERY", recoveryMs);
            log.info("[RECOVERY] {} STANDBY → ACTIVE ({}ms)", instanceId, recoveryMs);
        }
    }

    // ── timeout 감시 ─────────────────────────────────────────────────────────

    private void checkTimeouts() {
        long now       = System.currentTimeMillis();
        long threshold = settings.heartbeatTimeoutMs();
        for (Map.Entry<String, Long> entry : lastHeartbeatMs.entrySet()) {
            long silenceMs = now - entry.getValue();
            if (silenceMs > threshold) {
                log.warn("[TIMEOUT] {} — {}ms 침묵", entry.getKey(), silenceMs);
                timeoutHandler.handle(entry.getKey(), silenceMs);
            }
        }
    }

    // ── throughput 집계 flush ────────────────────────────────────────────────

    private void flushThroughput() {
        long now   = System.currentTimeMillis();
        long start = windowStart;
        windowStart = now;

        long hbCount = heartbeatCount.getAndSet(0);
        long stCount = statusCount.getAndSet(0);

        if (hbCount > 0) {
            repository.insertThroughput(settings.heartbeatTopic(), "hb-monitor-consumer", hbCount, start, now);
        }
        if (stCount > 0) {
            repository.insertThroughput(settings.statusTopic(), "status-monitor-consumer", stCount, start, now);
        }
        log.debug("[THROUGHPUT] hb={} status={} ({}ms window)", hbCount, stCount, now - start);
    }
}
