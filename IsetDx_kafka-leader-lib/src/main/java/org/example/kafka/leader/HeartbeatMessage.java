package org.example.kafka.leader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 인스턴스 생존 신호(heartbeat) 메시지.
 *
 * <p>weight 필드는 split-brain 발생 시 ACTIVE 우선순위 결정에 사용된다.
 * 값이 클수록 ACTIVE를 유지할 우선순위가 높다.
 * weight가 같으면 instanceId 사전순 비교 — 더 작은 ID가 ACTIVE를 유지한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HeartbeatMessage {

    private String instanceId;
    private long   timestamp;  // System.currentTimeMillis()
    private int    weight;     // 우선순위 가중치 (높을수록 ACTIVE 유지 우선)

    public HeartbeatMessage() {}

    public HeartbeatMessage(String instanceId, long timestamp, int weight) {
        this.instanceId = instanceId;
        this.timestamp  = timestamp;
        this.weight     = weight;
    }

    public String getInstanceId() { return instanceId; }
    public long   getTimestamp()  { return timestamp; }
    public int    getWeight()     { return weight; }
}
