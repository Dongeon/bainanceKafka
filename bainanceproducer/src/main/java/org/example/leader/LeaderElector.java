package org.example.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Active-Standby 리더 선출 상태 머신.
 *
 * <p>상태 전이:
 * <pre>
 *   PREPARING ──(외부 heartbeat 없음, prep 경과)──▶ ACTIVE
 *   PREPARING ──(외부 heartbeat 감지)────────────▶ STANDBY
 *   STANDBY   ──(15초 침묵)────────────────────▶ ACTIVE
 *   ACTIVE    ──(외부 ACTIVE 감지, instanceId 비교로 패자 결정)──▶ STANDBY
 * </pre>
 *
 * <p>동시 시작(split-brain) 방지:
 *   prep 구간에 5~7초 jitter를 추가하여 두 인스턴스가 동시에 ACTIVE 선언하는 확률을 최소화한다.
 *   그래도 충돌하면 instanceId 사전순 비교 — 더 작은 ID가 ACTIVE를 유지하고 큰 ID가 양보한다.
 */
public class LeaderElector {

    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);

    private enum State { PREPARING, STANDBY, ACTIVE }

    /** prep 후 외부 heartbeat가 없으면 즉시 ACTIVE 전환 */
    private static final long PREP_BASE_MS       = 5_000;
    private static final long PREP_JITTER_MS     = 2_000;

    /** STANDBY 중 마지막 외부 heartbeat로부터 이 시간이 지나면 ACTIVE 전환 */
    private static final long ACTIVATION_MS      = 15_000;

    /** 타임아웃 감지 주기 */
    private static final long CHECK_INTERVAL_MS  = 1_000;

    private volatile State state = State.PREPARING;

    /** 마지막으로 외부 heartbeat를 수신한 시각. -1 = 한 번도 수신 안 함 */
    private final AtomicLong lastForeignHeartbeatMs = new AtomicLong(-1);

    private final String instanceId;
    private final HeartbeatPublisher publisher;
    private final HeartbeatWatcher   watcher;
    private final Runnable onActivate;
    private final Runnable onDeactivate;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "leader-elector");
        t.setDaemon(true);
        return t;
    });

    public LeaderElector(String instanceId,
                         HeartbeatPublisher publisher,
                         HeartbeatWatcher watcher,
                         Runnable onActivate,
                         Runnable onDeactivate) {
        this.instanceId   = instanceId;
        this.publisher    = publisher;
        this.watcher      = watcher;
        this.onActivate   = onActivate;
        this.onDeactivate = onDeactivate;
    }

    /**
     * 리더 선출을 시작한다.
     * HeartbeatWatcher를 즉시 구동하고 prep 구간 후 최초 상태를 결정한다.
     */
    public void start() {
        log.info("[{}] LeaderElector starting (PREPARING)", instanceId);

        // HeartbeatWatcher 즉시 시작 — prep 동안 외부 heartbeat 감지
        watcher.start();

        // prep 종료 후 초기 상태 결정
        long prepDelay = PREP_BASE_MS + ThreadLocalRandom.current().nextLong(0, PREP_JITTER_MS);
        scheduler.schedule(this::afterPreparation, prepDelay, TimeUnit.MILLISECONDS);

        // STANDBY 중 타임아웃 주기 감시 (prep 이후부터 동작)
        scheduler.scheduleAtFixedRate(
                this::checkActivationTimeout,
                prepDelay + ACTIVATION_MS,
                CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        log.info("[{}] Preparation phase: {}ms (base {}ms + jitter)", instanceId, prepDelay, PREP_BASE_MS);
    }

    /** prep 구간 종료 — 외부 heartbeat 수신 여부로 초기 상태 결정 */
    private void afterPreparation() {
        if (lastForeignHeartbeatMs.get() == -1) {
            log.info("[{}] No foreign heartbeat during preparation → ACTIVE", instanceId);
            transitionTo(State.ACTIVE);
        } else {
            log.info("[{}] Foreign heartbeat detected during preparation → STANDBY", instanceId);
            transitionTo(State.STANDBY);
        }
    }

    /** STANDBY 중 주기적으로 타임아웃을 확인 */
    private void checkActivationTimeout() {
        if (state != State.STANDBY) return;
        long last = lastForeignHeartbeatMs.get();
        if (last == -1) return;
        long silence = System.currentTimeMillis() - last;
        if (silence > ACTIVATION_MS) {
            log.warn("[{}] No heartbeat for {}ms → ACTIVE", instanceId, silence);
            transitionTo(State.ACTIVE);
        }
    }

    /**
     * HeartbeatWatcher가 외부 인스턴스의 heartbeat를 수신하면 호출된다.
     *
     * <p>STANDBY: 타임아웃 리셋 (정상 동작 확인)
     * <p>ACTIVE: split-brain 감지 — instanceId 사전순 비교, 더 큰 쪽이 양보
     */
    public void onForeignHeartbeat(HeartbeatMessage msg) {
        lastForeignHeartbeatMs.set(System.currentTimeMillis());

        if (state == State.ACTIVE) {
            // 사전순으로 더 작은 instanceId가 우선권을 가짐
            if (msg.getInstanceId().compareTo(instanceId) < 0) {
                log.warn("[{}] Split-brain detected. Foreign {} has priority → STANDBY",
                        instanceId, msg.getInstanceId());
                transitionTo(State.STANDBY);
            }
        }
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

    public void shutdown() {
        scheduler.shutdownNow();
        watcher.stop();
        publisher.close();
        log.info("[{}] LeaderElector shut down", instanceId);
    }
}
