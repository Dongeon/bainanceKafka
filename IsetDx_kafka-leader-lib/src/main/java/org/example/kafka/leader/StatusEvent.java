package org.example.kafka.leader;

class StatusEvent {

    private String instanceId;
    private int    weight;
    private String state;
    private long   timestamp;

    StatusEvent() {}

    StatusEvent(String instanceId, int weight, String state, long timestamp) {
        this.instanceId = instanceId;
        this.weight     = weight;
        this.state      = state;
        this.timestamp  = timestamp;
    }

    public String getInstanceId() { return instanceId; }
    public int    getWeight()     { return weight; }
    public String getState()      { return state; }
    public long   getTimestamp()  { return timestamp; }
}
