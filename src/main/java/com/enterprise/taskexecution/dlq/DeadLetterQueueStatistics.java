package com.enterprise.taskexecution.dlq;

import java.time.Instant;
import java.util.Map;

/**
 * Statistics about the Dead Letter Queue.
 */
public class DeadLetterQueueStatistics {
    
    private final int currentSize;
    private final int totalAdded;
    private final int totalRemoved;
    private final Instant oldestEntryTime;
    private final Instant newestEntryTime;
    private final Map<String, Integer> errorTypeCounts;
    private final Map<String, Integer> taskTypeCounts;
    
    public DeadLetterQueueStatistics(int currentSize, int totalAdded, int totalRemoved,
                                   Instant oldestEntryTime, Instant newestEntryTime,
                                   Map<String, Integer> errorTypeCounts,
                                   Map<String, Integer> taskTypeCounts) {
        this.currentSize = currentSize;
        this.totalAdded = totalAdded;
        this.totalRemoved = totalRemoved;
        this.oldestEntryTime = oldestEntryTime;
        this.newestEntryTime = newestEntryTime;
        this.errorTypeCounts = errorTypeCounts;
        this.taskTypeCounts = taskTypeCounts;
    }
    
    public int getCurrentSize() {
        return currentSize;
    }
    
    public int getTotalAdded() {
        return totalAdded;
    }
    
    public int getTotalRemoved() {
        return totalRemoved;
    }
    
    public Instant getOldestEntryTime() {
        return oldestEntryTime;
    }
    
    public Instant getNewestEntryTime() {
        return newestEntryTime;
    }
    
    public Map<String, Integer> getErrorTypeCounts() {
        return errorTypeCounts;
    }
    
    public Map<String, Integer> getTaskTypeCounts() {
        return taskTypeCounts;
    }
    
    @Override
    public String toString() {
        return "DeadLetterQueueStatistics{" +
                "currentSize=" + currentSize +
                ", totalAdded=" + totalAdded +
                ", totalRemoved=" + totalRemoved +
                ", oldestEntryTime=" + oldestEntryTime +
                ", newestEntryTime=" + newestEntryTime +
                ", errorTypeCounts=" + errorTypeCounts +
                ", taskTypeCounts=" + taskTypeCounts +
                '}';
    }
}