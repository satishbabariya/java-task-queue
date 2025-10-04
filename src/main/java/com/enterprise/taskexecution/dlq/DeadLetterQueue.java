package com.enterprise.taskexecution.dlq;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Interface for Dead Letter Queue functionality.
 * Provides methods to manage failed tasks that have exceeded retry limits.
 */
public interface DeadLetterQueue {
    
    /**
     * Adds a task to the dead letter queue.
     * 
     * @param task The task to add to DLQ
     * @param failureReason The reason for the failure
     * @param lastAttemptTime The time of the last failed attempt
     * @return true if the task was successfully added to DLQ
     */
    boolean addToDeadLetterQueue(Task task, String failureReason, Instant lastAttemptTime);
    
    /**
     * Retrieves a task from the dead letter queue by ID.
     * 
     * @param taskId The ID of the task to retrieve
     * @return Optional containing the DLQ entry if found
     */
    Optional<DeadLetterEntry> getDeadLetterEntry(UUID taskId);
    
    /**
     * Retrieves all tasks in the dead letter queue.
     * 
     * @return List of all DLQ entries
     */
    List<DeadLetterEntry> getAllDeadLetterEntries();
    
    /**
     * Retrieves dead letter entries with pagination support.
     * 
     * @param offset Starting index for pagination
     * @param limit Maximum number of entries to return
     * @return List of DLQ entries within the specified range
     */
    List<DeadLetterEntry> getDeadLetterEntries(int offset, int limit);
    
    /**
     * Removes a task from the dead letter queue.
     * 
     * @param taskId The ID of the task to remove
     * @return true if the task was successfully removed
     */
    boolean removeFromDeadLetterQueue(UUID taskId);
    
    /**
     * Clears all entries from the dead letter queue.
     * 
     * @return Number of entries cleared
     */
    int clearDeadLetterQueue();
    
    /**
     * Gets the current size of the dead letter queue.
     * 
     * @return Number of entries in DLQ
     */
    int getDeadLetterQueueSize();
    
    /**
     * Checks if the dead letter queue is at capacity.
     * 
     * @return true if DLQ is at maximum capacity
     */
    boolean isAtCapacity();
    
    /**
     * Gets statistics about the dead letter queue.
     * 
     * @return DLQ statistics
     */
    DeadLetterQueueStatistics getStatistics();
    
    /**
     * Cleans up old entries based on retention policy.
     */
    void cleanupOldEntries();
    
    /**
     * Closes the dead letter queue and releases resources.
     */
    void close();
}