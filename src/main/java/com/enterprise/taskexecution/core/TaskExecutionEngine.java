package com.enterprise.taskexecution.core;

import com.enterprise.taskexecution.dlq.DeadLetterEntry;
import com.enterprise.taskexecution.dlq.DeadLetterQueueStatistics;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Main interface for the task execution engine.
 * Provides methods for submitting, scheduling, and managing tasks.
 */
public interface TaskExecutionEngine {
    
    /**
     * Submit a task for immediate execution
     */
    CompletableFuture<TaskResult> submit(Task task);
    
    /**
     * Schedule a task for future execution
     */
    CompletableFuture<Void> schedule(Task task);
    
    /**
     * Cancel a scheduled task
     */
    CompletableFuture<Boolean> cancel(UUID taskId);
    
    /**
     * Get the status of a task
     */
    CompletableFuture<TaskStatus> getTaskStatus(UUID taskId);
    
    /**
     * Get all tasks with a specific status
     */
    CompletableFuture<List<Task>> getTasksByStatus(TaskStatus status);
    
    /**
     * Register a task handler for a specific task type
     */
    void registerHandler(TaskHandler handler);
    
    /**
     * Unregister a task handler
     */
    void unregisterHandler(String taskType);
    
    /**
     * Start the execution engine
     */
    void start();
    
    /**
     * Stop the execution engine gracefully
     */
    CompletableFuture<Void> stop();
    
    /**
     * Check if the engine is running
     */
    boolean isRunning();
    
    /**
     * Get engine statistics
     */
    EngineStatistics getStatistics();
    
    // Dead Letter Queue Management Methods
    
    /**
     * Get a task from the dead letter queue by ID
     */
    CompletableFuture<Optional<DeadLetterEntry>> getDeadLetterEntry(UUID taskId);
    
    /**
     * Get all tasks in the dead letter queue
     */
    CompletableFuture<List<DeadLetterEntry>> getAllDeadLetterEntries();
    
    /**
     * Get dead letter entries with pagination
     */
    CompletableFuture<List<DeadLetterEntry>> getDeadLetterEntries(int offset, int limit);
    
    /**
     * Remove a task from the dead letter queue
     */
    CompletableFuture<Boolean> removeFromDeadLetterQueue(UUID taskId);
    
    /**
     * Clear all entries from the dead letter queue
     */
    CompletableFuture<Integer> clearDeadLetterQueue();
    
    /**
     * Get dead letter queue statistics
     */
    CompletableFuture<DeadLetterQueueStatistics> getDeadLetterQueueStatistics();
    
    /**
     * Retry a task from the dead letter queue
     */
    CompletableFuture<TaskResult> retryFromDeadLetterQueue(UUID taskId);
}