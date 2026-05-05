package org.example.handler;

import org.example.kafka.producer.GenericKafkaProducer;
import org.example.model.KlineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binance Kline 이벤트를 GenericKafkaProducer로 전달하는 핸들러.
 * is_closed=true인 확정 캔들만 전송하고, 진행 중인 캔들은 스킵한다.
 */
public class KlineEventHandler {

    private static final Logger log = LoggerFactory.getLogger(KlineEventHandler.class);
    private static final int STATUS_INTERVAL_SEC = 60;

    private final GenericKafkaProducer producer;
    private final String               topic;
    private final AtomicLong           sentCount    = new AtomicLong(0);
    private final AtomicLong           skippedCount = new AtomicLong(0);
    private final ScheduledExecutorService statusScheduler =
            Executors.newSingleThreadScheduledExecutor();

    public KlineEventHandler(GenericKafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic    = topic;
        statusScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                log.info("[KLINE STATUS] 전송 {}건 / 스킵(미확정) {}건",
                        sentCount.get(), skippedCount.get());
            }
        }, STATUS_INTERVAL_SEC, STATUS_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    public void handle(KlineEvent event) {
        if (!event.isClosed()) {
            skippedCount.incrementAndGet();
            return;
        }
        producer.send(topic, event);
        sentCount.incrementAndGet();
    }

    public void shutdown() {
        statusScheduler.shutdownNow();
    }
}
