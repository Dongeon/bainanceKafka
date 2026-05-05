package org.example.kafka.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Active-Standby 리더 선출 상태 머신.
 *
 * <p>상태 전이:
 * <pre>
 *   PREPARING ──(prep 경과, 외부 HB 없음)──▶ ACTIVE
 *   PREPARING ──(외부 HB 감지)─────────────▶ STANDBY
 *   STANDBY   ──(activationTimeout 침묵)───▶ ACTIVE
 *   ACTIVE    ──(split-brain, 우선순위 패배)▶ STANDBY
 * </pre>
 *
 * <p>weight 기반 split-brain 해소:
 * <pre>
 *   외부 weight > 내 weight  → 내가 STANDBY (외부가 우선순위 높음)
 *   외부 weight == 내 weight → instanceId 사전순 비교, 더 작은 쪽이 ACTIVE 유지
 *   외부 weight < 내 weight  → 내가 ACTIVE 유지
 * </pre>
 *
 * <p>동시 시작(split-brain) 1차 방어:
 *   prep 구간에 0~prepJitterMs 랜덤 지터를 추가한다.
 *   두 인스턴스가 동시에 ACTIVE가 될 확률을 최소화하며,
 *   충돌하더라도 weight 비교로 즉시 해소된다.
 *
 * <p>사용 예:
 * <pre>{@code
 *   LeaderElector elector = LeaderElector.fromConfig("kafka.conf")
 *       .onActivate(() -> producer.connect())
 *       .onDeactivate(() -> producer.disconnect())
 *       .build();
 *   elector.start();
 *
 *   // 종료 시
 *   elector.shutdown();
 * }</pre>
 */
public class LeaderElector {

    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);

    private enum State { PREPARING, STANDBY, ACTIVE }

    private final String   instanceId;
    private final int      ownWeight;
    private final Runnable onActivate;
    private final Runnable onDeactivate;

    private final long prepBaseMs;
    private final long prepJitterMs;
    private final long activationTimeoutMs;

    private volatile State state = State.PREPARING;

    /** 마지막으로 외부 heartbeat를 수신한 시각 (ms). -1 = 수신 이력 없음. */
    private final AtomicLong lastForeignHeartbeatMs = new AtomicLong(-1);

    private final HeartbeatPublisher publisher;
    private final HeartbeatWatcher   watcher;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "leader-elector");
            t.setDaemon(true);
            return t;
        }
    });

    // ── 생성자 (Builder를 통해서만 사용) ─────────────────────────────────────

    private LeaderElector(Builder builder) {
        LeaderSettings settings = builder.settings;

        this.instanceId          = settings.instanceId();
        this.ownWeight           = settings.instanceWeight();
        this.onActivate          = builder.onActivate;
        this.onDeactivate        = builder.onDeactivate;
        this.prepBaseMs          = settings.prepBaseMs();
        this.prepJitterMs        = settings.prepJitterMs();
        this.activationTimeoutMs = settings.activationTimeoutMs();

        this.publisher = new HeartbeatPublisher(settings);
        final LeaderElector self = this;
        this.watcher   = new HeartbeatWatcher(settings, new java.util.function.Consumer<HeartbeatMessage>() {
            public void accept(HeartbeatMessage msg) {
                self.onForeignHeartbeat(msg);
            }
        });
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    /** kafka.conf 경로로 Builder를 생성한다. */
    public static Builder fromConfig(String configPath) throws IOException {
        return new Builder(LeaderSettings.load(configPath));
    }

    /** 이미 로드된 LeaderSettings로 Builder를 생성한다. */
    public static Builder fromSettings(LeaderSettings settings) {
        return new Builder(settings);
    }

    public static class Builder {
        private final LeaderSettings settings;
        private Runnable onActivate   = new Runnable() { public void run() {} };
        private Runnable onDeactivate = new Runnable() { public void run() {} };

        private Builder(LeaderSettings settings) {
            this.settings = settings;
        }

        /** ACTIVE 전환 시 실행할 콜백 (예: WebSocket 연결 시작). */
        public Builder onActivate(Runnable onActivate) {
            this.onActivate = onActivate;
            return this;
        }

        /** STANDBY 전환 시 실행할 콜백 (예: WebSocket 연결 해제). */
        public Builder onDeactivate(Runnable onDeactivate) {
            this.onDeactivate = onDeactivate;
            return this;
        }

        public LeaderElector build() {
            return new LeaderElector(this);
        }
    }

    // ── 공개 API ─────────────────────────────────────────────────────────────

    /**
     * 리더 선출을 시작한다.
     * HeartbeatWatcher를 즉시 구동하고, prep 구간 후 초기 상태를 결정한다.
     */
    public void start() {
        log.info("[{}] LeaderElector starting (weight={}, prepBase={}ms, jitter={}ms, activationTimeout={}ms)",
                instanceId, ownWeight, prepBaseMs, prepJitterMs, activationTimeoutMs);

        watcher.start();

        long jitter    = ThreadLocalRandom.current().nextLong(0, prepJitterMs + 1);
        long prepDelay = prepBaseMs + jitter;

        scheduler.schedule(this::afterPreparation, prepDelay, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(
                this::checkActivationTimeout,
                prepDelay + activationTimeoutMs,
                1_000,
                TimeUnit.MILLISECONDS
        );

        log.info("[{}] Preparation phase: {}ms (base {}ms + jitter {}ms)",
                instanceId, prepDelay, prepBaseMs, jitter);
    }

    /** 리더 선출을 종료한다. 모든 내부 리소스를 정리한다. */
    public void shutdown() {
        scheduler.shutdownNow();
        watcher.stop();
        publisher.close();
        log.info("[{}] LeaderElector shut down", instanceId);
    }

    // ── 내부 상태 전이 ────────────────────────────────────────────────────────

    /** prep 구간 종료 — 외부 heartbeat 수신 여부로 초기 상태 결정. */
    private void afterPreparation() {
        if (lastForeignHeartbeatMs.get() == -1) {
            log.info("[{}] No foreign heartbeat during preparation → ACTIVE", instanceId);
            transitionTo(State.ACTIVE);
        } else {
            log.info("[{}] Foreign heartbeat detected during preparation → STANDBY", instanceId);
            transitionTo(State.STANDBY);
        }
    }

    /** STANDBY 중 1초마다 타임아웃 감시. */
    private void checkActivationTimeout() {
        if (state != State.STANDBY) return;
        long last = lastForeignHeartbeatMs.get();
        if (last == -1) return;
        long silence = System.currentTimeMillis() - last;
        if (silence > activationTimeoutMs) {
            log.warn("[{}] No heartbeat for {}ms → ACTIVE", instanceId, silence);
            transitionTo(State.ACTIVE);
        }
    }

    /**
     * HeartbeatWatcher가 외부 인스턴스의 heartbeat 수신 시 호출된다.
     *
     * <p>STANDBY: 타임아웃 리셋 (정상 동작 확인).
     * <p>ACTIVE:  split-brain 감지 — weight 우선, 동점 시 instanceId 사전순 비교.
     */
    private void onForeignHeartbeat(HeartbeatMessage msg) {
        lastForeignHeartbeatMs.set(System.currentTimeMillis());

        if (state != State.ACTIVE) return;

        boolean foreignHasPriority = hasPriorityOver(msg);
        if (foreignHasPriority) {
            log.warn("[{}] Split-brain: foreign {} (weight={}) has priority → STANDBY",
                    instanceId, msg.getInstanceId(), msg.getWeight());
            transitionTo(State.STANDBY);
        }
    }

    /**
     * 외부 인스턴스가 나보다 우선순위가 높은지 판단한다.
     *
     * <pre>
     *   외부 weight > 내 weight  → true  (외부 우선)
     *   외부 weight == 내 weight → instanceId 사전순, 더 작은 쪽이 우선
     *   외부 weight < 내 weight  → false (내가 우선)
     * </pre>
     */
    private boolean hasPriorityOver(HeartbeatMessage foreign) {
        if (foreign.getWeight() != ownWeight) {
            return foreign.getWeight() > ownWeight;
        }
        // weight 동점: 사전순으로 더 작은 instanceId가 ACTIVE 유지
        return foreign.getInstanceId().compareTo(instanceId) < 0;
    }

    private synchronized void transitionTo(State next) {
        if (state == next) return;
        State prev = state;
        state = next;
        log.info("[{}] {} → {}", instanceId, prev, next);

        if (next == State.ACTIVE) {
            publisher.start();
            onActivate.run();
        } else if (prev == State.ACTIVE) {
            publisher.stop();
            onDeactivate.run();
        }
    }
}
