package com.enterprise.taskexecution.core;

import java.time.Instant;
import java.util.Map;

/**
 * Statistics about the task execution engine
 */
public interface EngineStatistics {
    
    /**
     * Total number of tasks submitted
     */
    long getTotalTasksSubmitted();
    
    /**
     * Total number of tasks completed successfully
     */
    long getTotalTasksCompleted();
    
    /**
     * Total number of tasks failed
     */
    long getTotalTasksFailed();
    
    /**
     * Total number of tasks currently running
     */
    long getTasksRunning();
    
    /**
     * Total number of tasks pending execution
     */
    long getTasksPending();
    
    /**
     * Average task execution time in milliseconds
     */
    double getAverageExecutionTimeMs();
    
    /**
     * Engine uptime in milliseconds
     */
    long getUptimeMs();
    
    /**
     * When the engine was started
     */
    Instant getStartedAt();
    
    /**
     * Task counts by status
     */
    Map<TaskStatus, Long> getTaskCountsByStatus();
    
    /**
     * Task counts by type
     */
    Map<String, Long> getTaskCountsByType();
    
    /**
     * Queue size
     */
    int getQueueSize();
    
    /**
     * Active thread count
     */
    int getActiveThreadCount();
}