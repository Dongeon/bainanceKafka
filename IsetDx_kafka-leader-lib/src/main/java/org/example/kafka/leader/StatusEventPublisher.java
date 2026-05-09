package org.example.kafka.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 상태 전환 이벤트를 producer-status 토픽에 발행한다.
 *
 * <p>LeaderElector의 모든 상태 전환(PREPARING/ACTIVE/STANDBY/OFFLINE)을
 * 한 건씩 즉시 전송한다. consumer 측에서 정확한 상태 추적에 사용된다.
 */
class StatusEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(StatusEventPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String instanceId;
    private final int    weight;
    private final String topic;

    StatusEventPublisher(LeaderSettings settings) {
        this.instanceId = settings.instanceId();
        this.weight     = settings.instanceWeight();
        this.topic      = settings.statusEventTopic();
        this.producer   = new KafkaProducer<>(settings.toProducerProperties());
    }

    void publish(String state) {
        try {
            StatusEvent event = new StatusEvent(instanceId, weight, state, System.currentTimeMillis());
            String      json  = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, instanceId, json), new Callback() {
                public void onCompletion(RecordMetadata meta, Exception ex) {
                    if (ex != null) log.error("[{}] StatusEvent send failed: {}", instanceId, ex.getMessage());
                }
            });
            log.info("[{}] StatusEvent → {}", instanceId, state);
        } catch (Exception e) {
            log.error("[{}] StatusEvent error: {}", instanceId, e.getMessage());
        }
    }

    /** flush 후 닫는다. OFFLINE 이벤트가 유실되지 않도록 shutdown() 직후 호출. */
    void close() {
        producer.flush();
        producer.close();
    }
}
