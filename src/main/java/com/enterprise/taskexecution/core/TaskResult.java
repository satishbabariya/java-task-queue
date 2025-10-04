package com.enterprise.taskexecution.core;

import java.time.Instant;
import java.util.Map;

/**
 * Represents the result of a task execution
 */
public interface TaskResult {
    
    /**
     * Whether the task execution was successful
     */
    boolean isSuccess();
    
    /**
     * Error message if execution failed
     */
    String getErrorMessage();
    
    /**
     * Exception that caused the failure (if any)
     */
    Throwable getException();
    
    /**
     * Result data from successful execution
     */
    Map<String, Object> getResultData();
    
    /**
     * When the execution completed
     */
    Instant getCompletedAt();
    
    /**
     * Execution duration in milliseconds
     */
    long getExecutionDurationMs();
    
    /**
     * Whether this task should be retried
     */
    boolean shouldRetry();
    
    /**
     * Creates a successful result
     */
    static TaskResult success(Map<String, Object> resultData, long executionDurationMs) {
        return new TaskResultImpl(true, null, null, resultData, Instant.now(), executionDurationMs, false);
    }
    
    /**
     * Creates a failed result
     */
    static TaskResult failure(String errorMessage, Throwable exception, boolean shouldRetry, long executionDurationMs) {
        return new TaskResultImpl(false, errorMessage, exception, null, Instant.now(), executionDurationMs, shouldRetry);
    }
    
    /**
     * Default implementation of TaskResult
     */
    class TaskResultImpl implements TaskResult {
        private final boolean success;
        private final String errorMessage;
        private final Throwable exception;
        private final Map<String, Object> resultData;
        private final Instant completedAt;
        private final long executionDurationMs;
        private final boolean shouldRetry;
        
        public TaskResultImpl(boolean success, String errorMessage, Throwable exception, 
                            Map<String, Object> resultData, Instant completedAt, 
                            long executionDurationMs, boolean shouldRetry) {
            this.success = success;
            this.errorMessage = errorMessage;
            this.exception = exception;
            this.resultData = resultData;
            this.completedAt = completedAt;
            this.executionDurationMs = executionDurationMs;
            this.shouldRetry = shouldRetry;
        }
        
        @Override
        public boolean isSuccess() { return success; }
        
        @Override
        public String getErrorMessage() { return errorMessage; }
        
        @Override
        public Throwable getException() { return exception; }
        
        @Override
        public Map<String, Object> getResultData() { return resultData; }
        
        @Override
        public Instant getCompletedAt() { return completedAt; }
        
        @Override
        public long getExecutionDurationMs() { return executionDurationMs; }
        
        @Override
        public boolean shouldRetry() { return shouldRetry; }
    }
}