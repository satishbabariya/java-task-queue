package com.enterprise.taskexecution.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a task to be executed by the task execution engine.
 * Tasks are immutable and contain all necessary information for execution.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TaskImpl.class, name = "TaskImpl")
})
public interface Task {
    
    /**
     * Unique identifier for this task
     */
    UUID getId();
    
    /**
     * Task type identifier for routing to appropriate handler
     */
    String getType();
    
    /**
     * Task payload data
     */
    Map<String, Object> getPayload();
    
    /**
     * Priority level (higher number = higher priority)
     */
    int getPriority();
    
    /**
     * When this task was created
     */
    Instant getCreatedAt();
    
    /**
     * When this task should be executed (null for immediate execution)
     */
    Instant getScheduledFor();
    
    /**
     * Maximum number of retry attempts
     */
    int getMaxRetries();
    
    /**
     * Current retry count
     */
    int getRetryCount();
    
    /**
     * Delay between retries in milliseconds
     */
    long getRetryDelayMs();
    
    /**
     * Task execution timeout in milliseconds
     */
    long getTimeoutMs();
    
    /**
     * Additional metadata for the task
     */
    Map<String, Object> getMetadata();
    
    /**
     * Creates a new task with updated retry count
     */
    Task withRetryCount(int retryCount);
    
    /**
     * Creates a new task with updated scheduled time
     */
    Task withScheduledFor(Instant scheduledFor);
}