package com.enterprise.taskexecution.core;

import com.enterprise.taskexecution.dlq.DeadLetterEntry;
import com.enterprise.taskexecution.dlq.DeadLetterQueue;
import com.enterprise.taskexecution.dlq.DeadLetterQueueStatistics;
import com.enterprise.taskexecution.dlq.MapDBDeadLetterQueue;
import com.enterprise.taskexecution.exception.HandlerNotFoundException;
import com.enterprise.taskexecution.exception.TaskNotFoundException;
import com.enterprise.taskexecution.queue.PersistentTaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main implementation of the TaskExecutionEngine
 */
public class TaskExecutionEngineImpl implements TaskExecutionEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionEngineImpl.class);
    
    private final PersistentTaskQueue queue;
    private final AsyncTaskExecutor executor;
    private final DeadLetterQueue deadLetterQueue;
    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();
    private final Map<UUID, CompletableFuture<TaskResult>> runningTasks = new ConcurrentHashMap<>();
    private final Map<UUID, TaskStatus> finalStatuses = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalTasksSubmitted = new AtomicLong(0);
    private final Instant startedAt;
    
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> queueProcessor;
    private ScheduledFuture<?> scheduledTaskProcessor;
    
    public TaskExecutionEngineImpl(PersistentTaskQueue queue, AsyncTaskExecutor executor, DeadLetterQueue deadLetterQueue) {
        this.queue = queue;
        this.executor = executor;
        this.deadLetterQueue = deadLetterQueue;
        this.startedAt = Instant.now();
    }
    
    @Override
    public CompletableFuture<TaskResult> submit(Task task) {
        if (!running.get()) {
            throw new IllegalStateException("TaskExecutionEngine is not running");
        }
        
        totalTasksSubmitted.incrementAndGet();
        logger.debug("Submitting task {} of type {}", task.getId(), task.getType());
        
        // Add to queue
        queue.enqueue(task);
        
        // Start processing if not already running
        CompletableFuture<TaskResult> future = new CompletableFuture<>();
        runningTasks.put(task.getId(), future);
        
        return future;
    }
    
    @Override
    public CompletableFuture<Void> schedule(Task task) {
        if (!running.get()) {
            throw new IllegalStateException("TaskExecutionEngine is not running");
        }
        
        totalTasksSubmitted.incrementAndGet();
        logger.debug("Scheduling task {} of type {} for {}", 
                    task.getId(), task.getType(), task.getScheduledFor());
        
        queue.enqueue(task);
        
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<TaskResult> resultFuture = new CompletableFuture<>();
        runningTasks.put(task.getId(), resultFuture);
        
        resultFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(null);
            }
        });
        
        return future;
    }
    
    @Override
    public CompletableFuture<Boolean> cancel(UUID taskId) {
        logger.debug("Cancelling task {}", taskId);
        
        CompletableFuture<TaskResult> runningTask = runningTasks.get(taskId);
        if (runningTask != null) {
            runningTask.cancel(true);
            runningTasks.remove(taskId);
        }
        
        boolean removed = queue.removeTask(taskId);
        if (removed) {
            queue.updateTaskStatus(taskId, TaskStatus.CANCELLED);
            finalStatuses.put(taskId, TaskStatus.CANCELLED);
        }
        
        return CompletableFuture.completedFuture(removed);
    }
    
    @Override
    public CompletableFuture<TaskStatus> getTaskStatus(UUID taskId) {
        return CompletableFuture.supplyAsync(() -> {
            // If currently running
            if (runningTasks.containsKey(taskId)) {
                return TaskStatus.RUNNING;
            }
            // Terminal status if known
            TaskStatus terminal = finalStatuses.get(taskId);
            if (terminal != null) {
                return terminal;
            }
            Optional<Task> task = queue.getTaskById(taskId);
            if (task.isEmpty()) {
                // If not found in storage, but we have a terminal state recorded already handled above
                throw new RuntimeException(new TaskNotFoundException(taskId));
            }
            
            
            // Get status from queue
            List<Task> pendingTasks = queue.getTasksByStatus(TaskStatus.PENDING);
            List<Task> runningTasks = queue.getTasksByStatus(TaskStatus.RUNNING);
            List<Task> completedTasks = queue.getTasksByStatus(TaskStatus.COMPLETED);
            List<Task> failedTasks = queue.getTasksByStatus(TaskStatus.FAILED);
            List<Task> cancelledTasks = queue.getTasksByStatus(TaskStatus.CANCELLED);
            
            if (pendingTasks.stream().anyMatch(t -> t.getId().equals(taskId))) {
                return TaskStatus.PENDING;
            }
            if (runningTasks.stream().anyMatch(t -> t.getId().equals(taskId))) {
                return TaskStatus.RUNNING;
            }
            if (completedTasks.stream().anyMatch(t -> t.getId().equals(taskId))) {
                return TaskStatus.COMPLETED;
            }
            if (failedTasks.stream().anyMatch(t -> t.getId().equals(taskId))) {
                return TaskStatus.FAILED;
            }
            if (cancelledTasks.stream().anyMatch(t -> t.getId().equals(taskId))) {
                return TaskStatus.CANCELLED;
            }
            
            return TaskStatus.PENDING; // Default
        });
    }
    
    @Override
    public CompletableFuture<List<Task>> getTasksByStatus(TaskStatus status) {
        return CompletableFuture.completedFuture(queue.getTasksByStatus(status));
    }
    
    @Override
    public void registerHandler(TaskHandler handler) {
        String taskType = handler.getSupportedTaskType();
        handlers.put(taskType, handler);
        logger.info("Registered handler for task type: {}", taskType);
    }
    
    @Override
    public void unregisterHandler(String taskType) {
        TaskHandler removed = handlers.remove(taskType);
        if (removed != null) {
            logger.info("Unregistered handler for task type: {}", taskType);
        }
    }
    
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting TaskExecutionEngine...");
            
            // Start scheduler
            scheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "task-scheduler");
                t.setDaemon(false);
                return t;
            });
            
            // Start queue processor
            queueProcessor = scheduler.scheduleWithFixedDelay(
                this::processQueue, 0, 100, TimeUnit.MILLISECONDS);
            
            // Start scheduled task processor
            scheduledTaskProcessor = scheduler.scheduleWithFixedDelay(
                this::processScheduledTasks, 0, 1000, TimeUnit.MILLISECONDS);
            
            logger.info("TaskExecutionEngine started successfully");
        }
    }
    
    @Override
    public CompletableFuture<Void> stop() {
        return CompletableFuture.runAsync(() -> {
            if (running.compareAndSet(true, false)) {
                logger.info("Stopping TaskExecutionEngine...");
                
                // Stop schedulers
                if (queueProcessor != null) {
                    queueProcessor.cancel(false);
                }
                if (scheduledTaskProcessor != null) {
                    scheduledTaskProcessor.cancel(false);
                }
                
                // Shutdown scheduler
                if (scheduler != null) {
                    scheduler.shutdown();
                    try {
                        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                            scheduler.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                // Cancel all running tasks
                runningTasks.values().forEach(future -> future.cancel(true));
                runningTasks.clear();
                
                // Shutdown executor
                executor.shutdown().join();
                
                // Close queue
                queue.close();
                
                logger.info("TaskExecutionEngine stopped successfully");
            }
        });
    }
    
    @Override
    public boolean isRunning() {
        return running.get();
    }
    
    @Override
    public EngineStatistics getStatistics() {
        AsyncTaskExecutor.ExecutorStatistics executorStats = executor.getStatistics();
        
        return new EngineStatistics() {
            @Override
            public long getTotalTasksSubmitted() {
                return totalTasksSubmitted.get();
            }
            
            @Override
            public long getTotalTasksCompleted() {
                return executorStats.getTotalTasksCompleted();
            }
            
            @Override
            public long getTotalTasksFailed() {
                return executorStats.getTotalTasksFailed();
            }
            
            @Override
            public long getTasksRunning() {
                return queue.getTasksByStatus(TaskStatus.RUNNING).size();
            }
            
            @Override
            public long getTasksPending() {
                return queue.getTasksByStatus(TaskStatus.PENDING).size();
            }
            
            @Override
            public double getAverageExecutionTimeMs() {
                return executorStats.getAverageExecutionTimeMs();
            }
            
            @Override
            public long getUptimeMs() {
                return Instant.now().toEpochMilli() - startedAt.toEpochMilli();
            }
            
            @Override
            public Instant getStartedAt() {
                return startedAt;
            }
            
            @Override
            public Map<TaskStatus, Long> getTaskCountsByStatus() {
                Map<TaskStatus, Long> counts = new HashMap<>();
                for (TaskStatus status : TaskStatus.values()) {
                    counts.put(status, (long) queue.getTasksByStatus(status).size());
                }
                return counts;
            }
            
            @Override
            public Map<String, Long> getTaskCountsByType() {
                Map<String, Long> counts = new HashMap<>();
                handlers.keySet().forEach(type -> {
                    long count = queue.getTasksByStatus(TaskStatus.PENDING).stream()
                        .filter(task -> task.getType().equals(type))
                        .count();
                    counts.put(type, count);
                });
                return counts;
            }
            
            @Override
            public int getQueueSize() {
                return queue.size();
            }
            
            @Override
            public int getActiveThreadCount() {
                return executorStats.getActiveThreadCount();
            }
        };
    }
    
    private void processQueue() {
        try {
            Optional<Task> taskOpt = queue.dequeue();
            if (taskOpt.isPresent()) {
                Task task = taskOpt.get();
                processTask(task);
            }
        } catch (Exception e) {
            logger.error("Error processing queue", e);
        }
    }
    
    private void processScheduledTasks() {
        try {
            List<Task> scheduledTasks = queue.getScheduledTasks(Instant.now());
            for (Task task : scheduledTasks) {
                processTask(task);
            }
        } catch (Exception e) {
            logger.error("Error processing scheduled tasks", e);
        }
    }
    
    private void processTask(Task task) {
        TaskHandler handler = handlers.get(task.getType());
        if (handler == null) {
            logger.error("No handler found for task type: {}", task.getType());
            CompletableFuture<TaskResult> future = runningTasks.get(task.getId());
            if (future != null) {
                HandlerNotFoundException exception = new HandlerNotFoundException(task.getType());
                future.completeExceptionally(exception);
                runningTasks.remove(task.getId());
                
                // Move to DLQ if available
                if (deadLetterQueue != null) {
                    deadLetterQueue.addToDeadLetterQueue(task, 
                        "No handler found for task type: " + task.getType(), 
                        Instant.now());
                }
            }
            queue.updateTaskStatus(task.getId(), TaskStatus.FAILED);
            finalStatuses.put(task.getId(), TaskStatus.FAILED);
            return;
        }
        
        CompletableFuture<TaskResult> future = runningTasks.get(task.getId());
        if (future != null) {
            executor.execute(task, handler).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    // Terminal failure
                    future.completeExceptionally(throwable);
                    queue.updateTaskStatus(task.getId(), TaskStatus.FAILED);
                    finalStatuses.put(task.getId(), TaskStatus.FAILED);
                    runningTasks.remove(task.getId());
                    
                    // Move to DLQ if available
                    if (deadLetterQueue != null) {
                        deadLetterQueue.addToDeadLetterQueue(task, 
                            "Task execution failed: " + throwable.getMessage(), 
                            Instant.now());
                    }
                } else {
                    if (result.isSuccess()) {
                        future.complete(result);
                        queue.updateTaskStatus(task.getId(), TaskStatus.COMPLETED);
                        finalStatuses.put(task.getId(), TaskStatus.COMPLETED);
                        runningTasks.remove(task.getId());
                    } else if (result.shouldRetry() && task.getRetryCount() < task.getMaxRetries()) {
                        // Retry task: keep future pending, do not remove runningTasks
                        Task retryTask = task.withRetryCount(task.getRetryCount() + 1);
                        queue.enqueue(retryTask);
                        queue.updateTaskStatus(task.getId(), TaskStatus.RETRYING);
                        
                        // Schedule retry execution and handle its completion
                        executor.schedule(retryTask, handler, task.getRetryDelayMs())
                            .thenCompose(v -> executor.execute(retryTask, handler))
                            .whenComplete((retryResult, retryThrowable) -> {
                                // Complete the original future with the retry result
                                if (retryThrowable != null) {
                                    future.completeExceptionally(retryThrowable);
                                } else {
                                    future.complete(retryResult);
                                }
                                
                                // Update final status
                                if (retryResult != null && retryResult.isSuccess()) {
                                    queue.updateTaskStatus(task.getId(), TaskStatus.COMPLETED);
                                    finalStatuses.put(task.getId(), TaskStatus.COMPLETED);
                                } else {
                                    queue.updateTaskStatus(task.getId(), TaskStatus.FAILED);
                                    finalStatuses.put(task.getId(), TaskStatus.FAILED);
                                    
                                    // Move to DLQ if available
                                    if (deadLetterQueue != null) {
                                        String failureReason = retryThrowable != null ? 
                                            "Retry failed with exception: " + retryThrowable.getMessage() :
                                            "Retry failed: " + (retryResult != null ? retryResult.getErrorMessage() : "Unknown error");
                                        deadLetterQueue.addToDeadLetterQueue(task, failureReason, Instant.now());
                                    }
                                }
                                
                                runningTasks.remove(task.getId());
                            });
                    } else {
                        // Final failure - retries exhausted
                        future.complete(result);
                        queue.updateTaskStatus(task.getId(), TaskStatus.FAILED);
                        finalStatuses.put(task.getId(), TaskStatus.FAILED);
                        runningTasks.remove(task.getId());
                        
                        // Move to DLQ if available
                        if (deadLetterQueue != null) {
                            deadLetterQueue.addToDeadLetterQueue(task, 
                                "Task failed after " + task.getRetryCount() + " retries: " + 
                                (result != null ? result.getErrorMessage() : "Unknown error"), 
                                Instant.now());
                        }
                    }
                }
            });
        }
    }
    
    // Dead Letter Queue Management Methods
    
    @Override
    public CompletableFuture<Optional<DeadLetterEntry>> getDeadLetterEntry(UUID taskId) {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return Optional.empty();
            }
            return deadLetterQueue.getDeadLetterEntry(taskId);
        });
    }
    
    @Override
    public CompletableFuture<List<DeadLetterEntry>> getAllDeadLetterEntries() {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return List.of();
            }
            return deadLetterQueue.getAllDeadLetterEntries();
        });
    }
    
    @Override
    public CompletableFuture<List<DeadLetterEntry>> getDeadLetterEntries(int offset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return List.of();
            }
            return deadLetterQueue.getDeadLetterEntries(offset, limit);
        });
    }
    
    @Override
    public CompletableFuture<Boolean> removeFromDeadLetterQueue(UUID taskId) {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return false;
            }
            return deadLetterQueue.removeFromDeadLetterQueue(taskId);
        });
    }
    
    @Override
    public CompletableFuture<Integer> clearDeadLetterQueue() {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return 0;
            }
            return deadLetterQueue.clearDeadLetterQueue();
        });
    }
    
    @Override
    public CompletableFuture<DeadLetterQueueStatistics> getDeadLetterQueueStatistics() {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                return new DeadLetterQueueStatistics(0, 0, 0, null, null, Map.of(), Map.of());
            }
            return deadLetterQueue.getStatistics();
        });
    }
    
    @Override
    public CompletableFuture<TaskResult> retryFromDeadLetterQueue(UUID taskId) {
        return CompletableFuture.supplyAsync(() -> {
            if (deadLetterQueue == null) {
                throw new IllegalStateException("Dead Letter Queue is not enabled");
            }
            
            Optional<DeadLetterEntry> entry = deadLetterQueue.getDeadLetterEntry(taskId);
            if (entry.isEmpty()) {
                throw new IllegalArgumentException("Task not found in Dead Letter Queue: " + taskId);
            }
            
            DeadLetterEntry dlqEntry = entry.get();
            Task originalTask = dlqEntry.getOriginalTask();
            
            // Create a new task with reset retry count
            Task retryTask = originalTask.withRetryCount(0);
            
            // Remove from DLQ
            deadLetterQueue.removeFromDeadLetterQueue(taskId);
            
            // Submit for retry
            try {
                return submit(retryTask).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to retry task from DLQ", e);
            }
        });
    }
}