package com.enterprise.taskexecution.core;

/**
 * Represents the status of a task in the execution engine
 */
public enum TaskStatus {
    PENDING,        // Task is queued and waiting for execution
    RUNNING,        // Task is currently being executed
    COMPLETED,      // Task completed successfully
    FAILED,         // Task failed and won't be retried
    RETRYING,       // Task failed but will be retried
    CANCELLED,      // Task was cancelled
    TIMEOUT         // Task execution timed out
}