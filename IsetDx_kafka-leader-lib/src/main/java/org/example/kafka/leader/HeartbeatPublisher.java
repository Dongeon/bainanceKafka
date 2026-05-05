package org.example.kafka.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * ACTIVE 상태일 때 주기적으로 heartbeat를 Kafka 토픽에 발행한다.
 *
 * <p>생명주기:
 * <pre>
 *   start()  → ACTIVE 전환 시 호출. 즉시 첫 heartbeat 전송 후 주기 반복.
 *   stop()   → STANDBY 전환 시 호출. 스케줄러만 중단, Kafka 연결 유지.
 *   close()  → 종료 시 호출. 스케줄러 중단 + Kafka 연결 닫음.
 * </pre>
 */
class HeartbeatPublisher {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String instanceId;
    private final int    weight;
    private final String topic;
    private final long   intervalSec;

    private ScheduledExecutorService scheduler;

    HeartbeatPublisher(LeaderSettings settings) {
        this.instanceId  = settings.instanceId();
        this.weight      = settings.instanceWeight();
        this.topic       = settings.heartbeatTopic();
        this.intervalSec = settings.heartbeatIntervalSec();
        this.producer    = new KafkaProducer<>(settings.toProducerProperties());
    }

    /** ACTIVE 전환 시 호출. 즉시 첫 heartbeat 전송 후 intervalSec 주기로 반복. */
    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "heartbeat-publisher");
                t.setDaemon(true);
                return t;
            }
        });
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                send();
            }
        }, 0, intervalSec, TimeUnit.SECONDS);
        log.info("[{}] HeartbeatPublisher started (interval={}s, weight={})", instanceId, intervalSec, weight);
    }

    /** STANDBY 전환 시 호출. 스케줄러만 중단하고 Kafka 연결은 유지한다. */
    void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
            log.info("[{}] HeartbeatPublisher stopped", instanceId);
        }
    }

    /** 완전 종료. stop() 후 Kafka 연결을 닫는다. */
    void close() {
        stop();
        producer.flush();
        producer.close();
    }

    private void send() {
        try {
            HeartbeatMessage msg  = new HeartbeatMessage(instanceId, System.currentTimeMillis(), weight);
            String           json = mapper.writeValueAsString(msg);
            producer.send(new ProducerRecord<>(topic, instanceId, json), new Callback() {
                public void onCompletion(RecordMetadata meta, Exception ex) {
                    if (ex != null) log.error("[{}] Heartbeat send failed: {}", instanceId, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[{}] Heartbeat serialization error: {}", instanceId, e.getMessage());
        }
    }
}
