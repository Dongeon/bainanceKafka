package org.example.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * producer-heartbeat 토픽에서 수신하는 heartbeat 메시지 모델.
 * LeaderElector 의 HeartbeatMessage 와 동일한 JSON 구조.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HeartbeatEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String instanceId;
    private int    weight;
    private long   timestamp;

    public HeartbeatEvent() {}

    public HeartbeatEvent(String instanceId, int weight, long timestamp) {
        this.instanceId = instanceId;
        this.weight     = weight;
        this.timestamp  = timestamp;
    }

    public static HeartbeatEvent fromJson(String json) throws Exception {
        return MAPPER.readValue(json, HeartbeatEvent.class);
    }

    public String getInstanceId() { return instanceId; }
    public int    getWeight()     { return weight; }
    public long   getTimestamp()  { return timestamp; }
}
