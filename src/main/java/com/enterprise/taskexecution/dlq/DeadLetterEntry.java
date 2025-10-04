package com.enterprise.taskexecution.dlq;

import com.enterprise.taskexecution.core.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an entry in the Dead Letter Queue.
 * Contains the original task and metadata about its failure.
 */
public class DeadLetterEntry {
    
    private final UUID taskId;
    private final Task originalTask;
    private final String failureReason;
    private final Instant addedToDlqTime;
    private final Instant lastAttemptTime;
    private final int retryCount;
    private final String errorType;
    private final String stackTrace;
    
    @JsonCreator
    public DeadLetterEntry(
            @JsonProperty("taskId") UUID taskId,
            @JsonProperty("originalTask") Task originalTask,
            @JsonProperty("failureReason") String failureReason,
            @JsonProperty("addedToDlqTime") Instant addedToDlqTime,
            @JsonProperty("lastAttemptTime") Instant lastAttemptTime,
            @JsonProperty("retryCount") int retryCount,
            @JsonProperty("errorType") String errorType,
            @JsonProperty("stackTrace") String stackTrace) {
        this.taskId = Objects.requireNonNull(taskId, "Task ID cannot be null");
        this.originalTask = Objects.requireNonNull(originalTask, "Original task cannot be null");
        this.failureReason = Objects.requireNonNull(failureReason, "Failure reason cannot be null");
        this.addedToDlqTime = Objects.requireNonNull(addedToDlqTime, "Added to DLQ time cannot be null");
        this.lastAttemptTime = Objects.requireNonNull(lastAttemptTime, "Last attempt time cannot be null");
        this.retryCount = retryCount;
        this.errorType = errorType;
        this.stackTrace = stackTrace;
    }
    
    public UUID getTaskId() {
        return taskId;
    }
    
    public Task getOriginalTask() {
        return originalTask;
    }
    
    public String getFailureReason() {
        return failureReason;
    }
    
    public Instant getAddedToDlqTime() {
        return addedToDlqTime;
    }
    
    public Instant getLastAttemptTime() {
        return lastAttemptTime;
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    public String getErrorType() {
        return errorType;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
    
    /**
     * Creates a new DeadLetterEntry with the given parameters.
     */
    public static DeadLetterEntry create(Task task, String failureReason, Instant lastAttemptTime, 
                                       int retryCount, Throwable error) {
        return new DeadLetterEntry(
            task.getId(),
            task,
            failureReason,
            Instant.now(),
            lastAttemptTime,
            retryCount,
            error != null ? error.getClass().getSimpleName() : "Unknown",
            error != null ? getStackTrace(error) : null
        );
    }
    
    private static String getStackTrace(Throwable error) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        error.printStackTrace(pw);
        return sw.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeadLetterEntry that = (DeadLetterEntry) o;
        return Objects.equals(taskId, that.taskId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
    
    @Override
    public String toString() {
        return "DeadLetterEntry{" +
                "taskId=" + taskId +
                ", failureReason='" + failureReason + '\'' +
                ", addedToDlqTime=" + addedToDlqTime +
                ", retryCount=" + retryCount +
                ", errorType='" + errorType + '\'' +
                '}';
    }
}