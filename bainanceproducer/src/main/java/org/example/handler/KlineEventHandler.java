package org.example.handler;

import org.example.kafka.KafkaKlineProducer;
import org.example.model.KlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket에서 수신한 KlineEvent를 필터링하여 Kafka로 전달하는 핸들러.
 *
 * <p>핵심 정책: is_closed=true인 확정 캔들만 Kafka로 전송한다.
 * 진행 중인 캔들(is_closed=false)은 무시하여 DB 부하를 최소화한다.
 */
public class KlineEventHandler {

    private static final Logger log = LoggerFactory.getLogger(KlineEventHandler.class);
    private static final int STATUS_INTERVAL_SEC = 60;

    private final KafkaKlineProducer kafkaProducer;
    private final AtomicLong sentCount    = new AtomicLong(0);
    private final AtomicLong skippedCount = new AtomicLong(0);
    private final ScheduledExecutorService statusScheduler = Executors.newSingleThreadScheduledExecutor();

    public KlineEventHandler(KafkaKlineProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        startStatusLogger();
    }

    public void handle(KlineEvent event) {
        if (!event.isClosed()) {
            skippedCount.incrementAndGet();
            return;
        }
        kafkaProducer.send(event);
        sentCount.incrementAndGet();
    }

    private void startStatusLogger() {
        statusScheduler.scheduleAtFixedRate(() ->
            log.info("[KLINE STATUS] 전송 {}건 / 스킵(미확정) {}건", sentCount.get(), skippedCount.get()),
            STATUS_INTERVAL_SEC, STATUS_INTERVAL_SEC, TimeUnit.SECONDS
        );
    }

    public void shutdown() {
        statusScheduler.shutdownNow();
    }
}
