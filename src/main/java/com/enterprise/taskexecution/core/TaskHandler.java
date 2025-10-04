package com.enterprise.taskexecution.core;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for handling task execution.
 * Implementations should be thread-safe and handle exceptions gracefully.
 */
public interface TaskHandler {
    
    /**
     * Handle the execution of a task
     * @param task The task to execute
     * @return CompletableFuture that completes when the task is done
     */
    CompletableFuture<TaskResult> handle(Task task);
    
    /**
     * Get the task type this handler supports
     */
    String getSupportedTaskType();
    
    /**
     * Check if this handler can handle the given task
     */
    default boolean canHandle(Task task) {
        return getSupportedTaskType().equals(task.getType());
    }
}