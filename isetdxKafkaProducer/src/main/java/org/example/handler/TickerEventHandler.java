package org.example.handler;

import org.example.kafka.producer.GenericKafkaProducer;
import org.example.model.TickerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binance Ticker 이벤트를 GenericKafkaProducer로 전달하는 핸들러.
 * 60초마다 누적 전송 건수를 로그로 출력한다.
 */
public class TickerEventHandler {

    private static final Logger log = LoggerFactory.getLogger(TickerEventHandler.class);
    private static final int STATUS_INTERVAL_SEC = 60;

    private final GenericKafkaProducer producer;
    private final String               topic;
    private final AtomicLong           messageCount = new AtomicLong(0);
    private final ScheduledExecutorService statusScheduler =
            Executors.newSingleThreadScheduledExecutor();

    public TickerEventHandler(GenericKafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic    = topic;
        statusScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                log.info("[TICKER STATUS] 누적 전송 {}건", messageCount.get());
            }
        }, STATUS_INTERVAL_SEC, STATUS_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    public void handle(TickerEvent event) {
        producer.send(topic, event);
        messageCount.incrementAndGet();
    }

    public void shutdown() {
        statusScheduler.shutdownNow();
    }
}
