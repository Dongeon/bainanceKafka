package org.example.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ACTIVE 상태일 때 5초마다 producer-heartbeat 토픽에 생존 신호를 전송한다.
 *
 * <p>Standby 인스턴스는 이 메시지를 HeartbeatWatcher로 구독하여
 * 15초 이상 수신이 없으면 Active 인스턴스가 죽었다고 판단한다.
 */
public class HeartbeatPublisher {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatPublisher.class);
    static final String TOPIC = "producer-heartbeat";
    private static final long INTERVAL_SEC = 5;

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String instanceId;

    private ScheduledExecutorService scheduler;

    public HeartbeatPublisher(String bootstrapServers, String instanceId) {
        this.instanceId = instanceId;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        this.producer = new KafkaProducer<>(props);
    }

    /** ACTIVE 전환 시 호출 — 즉시 첫 heartbeat 전송 후 5초 주기로 반복 */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-publisher");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::send, 0, INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("[{}] HeartbeatPublisher started", instanceId);
    }

    /** STANDBY 전환 또는 종료 시 호출 — 스케줄러만 멈추고 Kafka 연결은 유지 */
    public void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
            log.info("[{}] HeartbeatPublisher stopped", instanceId);
        }
    }

    public void close() {
        stop();
        producer.flush();
        producer.close();
    }

    private void send() {
        try {
            HeartbeatMessage msg = new HeartbeatMessage(instanceId, System.currentTimeMillis());
            String json = mapper.writeValueAsString(msg);
            producer.send(new ProducerRecord<>(TOPIC, instanceId, json), (meta, ex) -> {
                if (ex != null) log.error("[{}] Heartbeat send failed: {}", instanceId, ex.getMessage());
            });
        } catch (Exception e) {
            log.error("[{}] Heartbeat serialization error: {}", instanceId, e.getMessage());
        }
    }
}
