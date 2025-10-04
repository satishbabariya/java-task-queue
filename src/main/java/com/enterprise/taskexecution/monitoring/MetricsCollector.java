package com.enterprise.taskexecution.monitoring;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskResult;
import com.enterprise.taskexecution.core.TaskStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes metrics for the task execution engine
 */
public class MetricsCollector {
    
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, Counter> taskTypeCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> taskTypeTimers = new ConcurrentHashMap<>();
    
    // Core metrics
    private final Counter tasksSubmitted;
    private final Counter tasksCompleted;
    private final Counter tasksFailed;
    private final Counter tasksRetried;
    private final Counter tasksCancelled;
    private final Counter tasksTimedOut;
    private final Counter tasksMovedToDlq;
    private final Counter tasksRetriedFromDlq;
    
    private final Timer taskExecutionTime;
    private final Timer taskQueueTime;
    
    // Gauges
    private final AtomicLong queueSize = new AtomicLong(0);
    private final AtomicLong runningTasks = new AtomicLong(0);
    private final AtomicLong pendingTasks = new AtomicLong(0);
    private final AtomicLong dlqSize = new AtomicLong(0);
    
    public MetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize counters
        this.tasksSubmitted = Counter.builder("taskexecution.tasks.submitted")
            .description("Total number of tasks submitted")
            .register(meterRegistry);
            
        this.tasksCompleted = Counter.builder("taskexecution.tasks.completed")
            .description("Total number of tasks completed successfully")
            .register(meterRegistry);
            
        this.tasksFailed = Counter.builder("taskexecution.tasks.failed")
            .description("Total number of tasks that failed")
            .register(meterRegistry);
            
        this.tasksRetried = Counter.builder("taskexecution.tasks.retried")
            .description("Total number of task retries")
            .register(meterRegistry);
            
        this.tasksCancelled = Counter.builder("taskexecution.tasks.cancelled")
            .description("Total number of tasks cancelled")
            .register(meterRegistry);
            
        this.tasksTimedOut = Counter.builder("taskexecution.tasks.timedout")
            .description("Total number of tasks that timed out")
            .register(meterRegistry);
            
        this.tasksMovedToDlq = Counter.builder("taskexecution.tasks.moved.to.dlq")
            .description("Total number of tasks moved to Dead Letter Queue")
            .register(meterRegistry);
            
        this.tasksRetriedFromDlq = Counter.builder("taskexecution.tasks.retried.from.dlq")
            .description("Total number of tasks retried from Dead Letter Queue")
            .register(meterRegistry);
        
        // Initialize timers
        this.taskExecutionTime = Timer.builder("taskexecution.task.execution.time")
            .description("Task execution time")
            .register(meterRegistry);
            
        this.taskQueueTime = Timer.builder("taskexecution.task.queue.time")
            .description("Time tasks spend in queue")
            .register(meterRegistry);
        
        // Initialize gauges
        Gauge.builder("taskexecution.queue.size", queueSize, AtomicLong::get)
            .description("Current queue size")
            .register(meterRegistry);
            
        Gauge.builder("taskexecution.tasks.running", runningTasks, AtomicLong::get)
            .description("Number of currently running tasks")
            .register(meterRegistry);
            
        Gauge.builder("taskexecution.tasks.pending", pendingTasks, AtomicLong::get)
            .description("Number of pending tasks")
            .register(meterRegistry);
            
        Gauge.builder("taskexecution.dlq.size", dlqSize, AtomicLong::get)
            .description("Current Dead Letter Queue size")
            .register(meterRegistry);
        
        logger.info("MetricsCollector initialized");
    }
    
    /**
     * Record a task submission
     */
    public void recordTaskSubmitted(Task task) {
        tasksSubmitted.increment();
        getTaskTypeCounter(task.getType(), "submitted").increment();
        
        logger.debug("Recorded task submission: {}", task.getId());
    }
    
    /**
     * Record a task completion
     */
    public void recordTaskCompleted(Task task, TaskResult result, long executionTimeMs) {
        tasksCompleted.increment();
        getTaskTypeCounter(task.getType(), "completed").increment();
        
        taskExecutionTime.record(executionTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        getTaskTypeTimer(task.getType()).record(executionTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        
        logger.debug("Recorded task completion: {} in {}ms", task.getId(), executionTimeMs);
    }
    
    /**
     * Record a task failure
     */
    public void recordTaskFailed(Task task, TaskResult result, long executionTimeMs) {
        tasksFailed.increment();
        getTaskTypeCounter(task.getType(), "failed").increment();
        
        taskExecutionTime.record(executionTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        getTaskTypeTimer(task.getType()).record(executionTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        
        if (result.getException() instanceof java.util.concurrent.TimeoutException) {
            tasksTimedOut.increment();
            getTaskTypeCounter(task.getType(), "timedout").increment();
        }
        
        logger.debug("Recorded task failure: {} - {}", task.getId(), result.getErrorMessage());
    }
    
    /**
     * Record a task retry
     */
    public void recordTaskRetry(Task task) {
        tasksRetried.increment();
        getTaskTypeCounter(task.getType(), "retried").increment();
        
        logger.debug("Recorded task retry: {} (attempt {})", task.getId(), task.getRetryCount());
    }
    
    /**
     * Record a task cancellation
     */
    public void recordTaskCancelled(Task task) {
        tasksCancelled.increment();
        getTaskTypeCounter(task.getType(), "cancelled").increment();
        
        logger.debug("Recorded task cancellation: {}", task.getId());
    }
    
    /**
     * Record queue time for a task
     */
    public void recordQueueTime(Task task, long queueTimeMs) {
        taskQueueTime.record(queueTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        
        logger.debug("Recorded queue time for task {}: {}ms", task.getId(), queueTimeMs);
    }
    
    /**
     * Update queue size gauge
     */
    public void updateQueueSize(int size) {
        queueSize.set(size);
    }
    
    /**
     * Update running tasks gauge
     */
    public void updateRunningTasks(int count) {
        runningTasks.set(count);
    }
    
    /**
     * Update pending tasks gauge
     */
    public void updatePendingTasks(int count) {
        pendingTasks.set(count);
    }
    
    /**
     * Update DLQ size gauge
     */
    public void updateDlqSize(int size) {
        dlqSize.set(size);
    }
    
    /**
     * Record a task moved to DLQ
     */
    public void recordTaskMovedToDlq(Task task, String reason) {
        tasksMovedToDlq.increment();
        getTaskTypeCounter(task.getType(), "moved_to_dlq").increment();
        
        logger.debug("Recorded task moved to DLQ: {} - {}", task.getId(), reason);
    }
    
    /**
     * Record a task retried from DLQ
     */
    public void recordTaskRetriedFromDlq(Task task) {
        tasksRetriedFromDlq.increment();
        getTaskTypeCounter(task.getType(), "retried_from_dlq").increment();
        
        logger.debug("Recorded task retried from DLQ: {}", task.getId());
    }
    
    /**
     * Record a custom metric
     */
    public void recordCustomMetric(String name, String description, double value, String... tags) {
        Gauge.builder(name, () -> value)
            .description(description)
            .tags(tags)
            .register(meterRegistry);
    }
    
    /**
     * Get task type specific counter
     */
    private Counter getTaskTypeCounter(String taskType, String status) {
        String key = taskType + "." + status;
        return taskTypeCounters.computeIfAbsent(key, k -> 
            Counter.builder("taskexecution.task.type")
                .tag("type", taskType)
                .tag("status", status)
                .description("Task count by type and status")
                .register(meterRegistry)
        );
    }
    
    /**
     * Get task type specific timer
     */
    private Timer getTaskTypeTimer(String taskType) {
        return taskTypeTimers.computeIfAbsent(taskType, k ->
            Timer.builder("taskexecution.task.type.execution.time")
                .tag("type", taskType)
                .description("Task execution time by type")
                .register(meterRegistry)
        );
    }
    
    /**
     * Get all metrics as a map
     */
    public java.util.Map<String, Object> getMetrics() {
        java.util.Map<String, Object> metrics = new ConcurrentHashMap<>();
        
        metrics.put("tasks.submitted", tasksSubmitted.count());
        metrics.put("tasks.completed", tasksCompleted.count());
        metrics.put("tasks.failed", tasksFailed.count());
        metrics.put("tasks.retried", tasksRetried.count());
        metrics.put("tasks.cancelled", tasksCancelled.count());
        metrics.put("tasks.timedout", tasksTimedOut.count());
        metrics.put("tasks.moved_to_dlq", tasksMovedToDlq.count());
        metrics.put("tasks.retried_from_dlq", tasksRetriedFromDlq.count());
        
        metrics.put("task.execution.time.mean", taskExecutionTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS));
        metrics.put("task.execution.time.max", taskExecutionTime.max(java.util.concurrent.TimeUnit.MILLISECONDS));
        metrics.put("task.queue.time.mean", taskQueueTime.mean(java.util.concurrent.TimeUnit.MILLISECONDS));
        
        metrics.put("queue.size", queueSize.get());
        metrics.put("tasks.running", runningTasks.get());
        metrics.put("tasks.pending", pendingTasks.get());
        metrics.put("dlq.size", dlqSize.get());
        
        return metrics;
    }
}