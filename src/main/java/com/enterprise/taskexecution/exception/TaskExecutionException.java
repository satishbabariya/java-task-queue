package com.enterprise.taskexecution.exception;

/**
 * Base exception for task execution related errors
 */
public class TaskExecutionException extends Exception {
    
    public TaskExecutionException(String message) {
        super(message);
    }
    
    public TaskExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}