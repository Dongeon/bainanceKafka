package org.example.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * producer-status 토픽에서 수신하는 상태 전환 이벤트 모델.
 * LeaderElector 의 StatusEventPublisher 가 발행하는 JSON 과 동일한 구조.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatusEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String instanceId;
    private int    weight;
    private String state;      // PREPARING | ACTIVE | STANDBY | OFFLINE
    private long   timestamp;

    public StatusEvent() {}

    public StatusEvent(String instanceId, int weight, String state, long timestamp) {
        this.instanceId = instanceId;
        this.weight     = weight;
        this.state      = state;
        this.timestamp  = timestamp;
    }

    public static StatusEvent fromJson(String json) throws Exception {
        return MAPPER.readValue(json, StatusEvent.class);
    }

    public String getInstanceId() { return instanceId; }
    public int    getWeight()     { return weight; }
    public String getState()      { return state; }
    public long   getTimestamp()  { return timestamp; }
}
