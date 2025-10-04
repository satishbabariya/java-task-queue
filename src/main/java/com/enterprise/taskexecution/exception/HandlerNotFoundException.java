package com.enterprise.taskexecution.exception;

/**
 * Exception thrown when no handler is found for a task type
 */
public class HandlerNotFoundException extends TaskExecutionException {
    
    private final String taskType;
    
    public HandlerNotFoundException(String taskType) {
        super("No handler found for task type: " + taskType);
        this.taskType = taskType;
    }
    
    public String getTaskType() {
        return taskType;
    }
}