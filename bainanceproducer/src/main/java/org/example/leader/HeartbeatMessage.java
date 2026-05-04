package org.example.leader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeartbeatMessage {

    private String instanceId;
    private long   timestamp;   // System.currentTimeMillis()

    public HeartbeatMessage() {}

    public HeartbeatMessage(String instanceId, long timestamp) {
        this.instanceId = instanceId;
        this.timestamp  = timestamp;
    }

    public String getInstanceId() { return instanceId; }
    public long   getTimestamp()  { return timestamp; }
}
