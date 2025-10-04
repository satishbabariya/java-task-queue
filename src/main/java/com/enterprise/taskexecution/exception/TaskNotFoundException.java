package com.enterprise.taskexecution.exception;

import java.util.UUID;

/**
 * Exception thrown when a requested task is not found
 */
public class TaskNotFoundException extends TaskExecutionException {
    
    private final UUID taskId;
    
    public TaskNotFoundException(UUID taskId) {
        super("Task not found: " + taskId);
        this.taskId = taskId;
    }
    
    public UUID getTaskId() {
        return taskId;
    }
}