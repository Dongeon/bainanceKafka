package org.example.model;

import java.time.LocalDateTime;

public class ProducerStatus {

    private String instanceId;
    private int weight;
    private String state;
    private LocalDateTime receivedAt;
    private LocalDateTime updatedAt;

    public ProducerStatus(String instanceId, int weight, String state,
                          LocalDateTime receivedAt, LocalDateTime updatedAt) {
        this.instanceId = instanceId;
        this.weight     = weight;
        this.state      = state;
        this.receivedAt = receivedAt;
        this.updatedAt  = updatedAt;
    }

    public String        getInstanceId() { return instanceId; }
    public int           getWeight()     { return weight; }
    public String        getState()      { return state; }
    public LocalDateTime getReceivedAt() { return receivedAt; }
    public LocalDateTime getUpdatedAt()  { return updatedAt; }
}
