package com.enterprise.taskexecution.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of the Task interface
 */
public class TaskImpl implements Task {
    
    private final UUID id;
    private final String type;
    private final Map<String, Object> payload;
    private final int priority;
    private final Instant createdAt;
    private final Instant scheduledFor;
    private final int maxRetries;
    private final int retryCount;
    private final long retryDelayMs;
    private final long timeoutMs;
    private final Map<String, Object> metadata;
    
    @JsonCreator
    public TaskImpl(@JsonProperty("id") UUID id,
                   @JsonProperty("type") String type,
                   @JsonProperty("payload") Map<String, Object> payload,
                   @JsonProperty("priority") int priority,
                   @JsonProperty("createdAt") Instant createdAt,
                   @JsonProperty("scheduledFor") Instant scheduledFor,
                   @JsonProperty("maxRetries") int maxRetries,
                   @JsonProperty("retryCount") int retryCount,
                   @JsonProperty("retryDelayMs") long retryDelayMs,
                   @JsonProperty("timeoutMs") long timeoutMs,
                   @JsonProperty("metadata") Map<String, Object> metadata) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.priority = priority;
        this.createdAt = createdAt;
        this.scheduledFor = scheduledFor;
        this.maxRetries = maxRetries;
        this.retryCount = retryCount;
        this.retryDelayMs = retryDelayMs;
        this.timeoutMs = timeoutMs;
        this.metadata = metadata;
    }
    
    @Override
    public UUID getId() { return id; }
    
    @Override
    public String getType() { return type; }
    
    @Override
    public Map<String, Object> getPayload() { return payload; }
    
    @Override
    public int getPriority() { return priority; }
    
    @Override
    public Instant getCreatedAt() { return createdAt; }
    
    @Override
    public Instant getScheduledFor() { return scheduledFor; }
    
    @Override
    public int getMaxRetries() { return maxRetries; }
    
    @Override
    public int getRetryCount() { return retryCount; }
    
    @Override
    public long getRetryDelayMs() { return retryDelayMs; }
    
    @Override
    public long getTimeoutMs() { return timeoutMs; }
    
    @Override
    public Map<String, Object> getMetadata() { return metadata; }
    
    @Override
    public Task withRetryCount(int retryCount) {
        return new TaskImpl(id, type, payload, priority, createdAt, scheduledFor,
                           maxRetries, retryCount, retryDelayMs, timeoutMs, metadata);
    }
    
    @Override
    public Task withScheduledFor(Instant scheduledFor) {
        return new TaskImpl(id, type, payload, priority, createdAt, scheduledFor,
                           maxRetries, retryCount, retryDelayMs, timeoutMs, metadata);
    }
    
    /**
     * Builder for creating Task instances
     */
    public static class Builder {
        private UUID id = UUID.randomUUID();
        private String type;
        private Map<String, Object> payload;
        private int priority = 0;
        private Instant createdAt = Instant.now();
        private Instant scheduledFor;
        private int maxRetries = 3;
        private int retryCount = 0;
        private long retryDelayMs = 5000; // 5 seconds
        private long timeoutMs = 30000; // 30 seconds
        private Map<String, Object> metadata;
        
        public Builder id(UUID id) {
            this.id = id;
            return this;
        }
        
        public Builder type(String type) {
            this.type = type;
            return this;
        }
        
        public Builder payload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }
        
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder scheduledFor(Instant scheduledFor) {
            this.scheduledFor = scheduledFor;
            return this;
        }
        
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }
        
        public Builder retryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
            return this;
        }
        
        public Builder timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }
        
        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }
        
        public Task build() {
            if (type == null) {
                throw new IllegalArgumentException("Task type is required");
            }
            return new TaskImpl(id, type, payload, priority, createdAt, scheduledFor,
                               maxRetries, retryCount, retryDelayMs, timeoutMs, metadata);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
}