package com.enterprise.taskexecution.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async task executor with thread pool management and monitoring
 */
public class AsyncTaskExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncTaskExecutor.class);
    
    private final ThreadPoolExecutor executor;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong totalTasksExecuted = new AtomicLong(0);
    private final AtomicLong totalTasksCompleted = new AtomicLong(0);
    private final AtomicLong totalTasksFailed = new AtomicLong(0);
    private final AtomicLong totalExecutionTime = new AtomicLong(0);
    
    public AsyncTaskExecutor(int corePoolSize, int maximumPoolSize, 
                           long keepAliveTime, TimeUnit unit,
                           BlockingQueue<Runnable> workQueue) {
        
        this.executor = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            workQueue,
            new TaskThreadFactory(),
            new TaskRejectedExecutionHandler()
        );
        
        this.scheduler = Executors.newScheduledThreadPool(2, new TaskThreadFactory());
        
        logger.info("AsyncTaskExecutor initialized with core={}, max={}, keepAlive={}ms", 
                   corePoolSize, maximumPoolSize, keepAliveTime);
    }
    
    /**
     * Execute a task asynchronously
     */
    public CompletableFuture<TaskResult> execute(Task task, TaskHandler handler) {
        totalTasksExecuted.incrementAndGet();
        
        CompletableFuture<TaskResult> future = CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            try {
                logger.debug("Executing task {} of type {}", task.getId(), task.getType());
                
                // Execute with timeout. Ensure handler execution is also bounded when handler blocks
                CompletableFuture<TaskResult> handlerFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        return handler.handle(task).get();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executor);
                TaskResult result = handlerFuture.get(task.getTimeoutMs(), TimeUnit.MILLISECONDS);
                
                long executionTime = System.currentTimeMillis() - startTime;
                totalExecutionTime.addAndGet(executionTime);
                
                if (result.isSuccess()) {
                    totalTasksCompleted.incrementAndGet();
                    logger.debug("Task {} completed successfully in {}ms", task.getId(), executionTime);
                } else {
                    totalTasksFailed.incrementAndGet();
                    logger.warn("Task {} failed: {}", task.getId(), result.getErrorMessage());
                }
                
                return result;
                
            } catch (TimeoutException e) {
                totalTasksFailed.incrementAndGet();
                long executionTime = System.currentTimeMillis() - startTime;
                logger.error("Task {} timed out after {}ms", task.getId(), executionTime);
                return TaskResult.failure("Task execution timed out", e, true, executionTime);
                
            } catch (Exception e) {
                totalTasksFailed.incrementAndGet();
                long executionTime = System.currentTimeMillis() - startTime;
                logger.error("Task {} execution failed", task.getId(), e);
                return TaskResult.failure("Task execution failed: " + e.getMessage(), e, true, executionTime);
            }
        }, executor);
        
        return future;
    }
    
    /**
     * Schedule a task for delayed execution
     */
    public CompletableFuture<Void> schedule(Task task, TaskHandler handler, long delayMs) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        scheduler.schedule(() -> {
            try {
                execute(task, handler).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
        
        return future;
    }
    
    /**
     * Get executor statistics
     */
    public ExecutorStatistics getStatistics() {
        return new ExecutorStatistics() {
            @Override
            public long getTotalTasksExecuted() {
                return totalTasksExecuted.get();
            }
            
            @Override
            public long getTotalTasksCompleted() {
                return totalTasksCompleted.get();
            }
            
            @Override
            public long getTotalTasksFailed() {
                return totalTasksFailed.get();
            }
            
            @Override
            public double getAverageExecutionTimeMs() {
                long executed = totalTasksExecuted.get();
                return executed > 0 ? (double) totalExecutionTime.get() / executed : 0.0;
            }
            
            @Override
            public int getActiveThreadCount() {
                return executor.getActiveCount();
            }
            
            @Override
            public int getPoolSize() {
                return executor.getPoolSize();
            }
            
            @Override
            public int getCorePoolSize() {
                return executor.getCorePoolSize();
            }
            
            @Override
            public int getMaximumPoolSize() {
                return executor.getMaximumPoolSize();
            }
            
            @Override
            public long getCompletedTaskCount() {
                return executor.getCompletedTaskCount();
            }
            
            @Override
            public int getQueueSize() {
                return executor.getQueue().size();
            }
        };
    }
    
    /**
     * Shutdown the executor gracefully
     */
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.runAsync(() -> {
            logger.info("Shutting down AsyncTaskExecutor...");
            
            executor.shutdown();
            scheduler.shutdown();
            
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate gracefully, forcing shutdown");
                    executor.shutdownNow();
                }
                
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Scheduler did not terminate gracefully, forcing shutdown");
                    scheduler.shutdownNow();
                }
                
                logger.info("AsyncTaskExecutor shutdown completed");
                
            } catch (InterruptedException e) {
                logger.error("Interrupted during shutdown", e);
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Check if executor is running
     */
    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated();
    }
    
    /**
     * Custom thread factory for task execution threads
     */
    private static class TaskThreadFactory implements ThreadFactory {
        private final AtomicLong threadNumber = new AtomicLong(1);
        private final String namePrefix = "task-executor-";
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
    
    /**
     * Custom rejected execution handler
     */
    private static class TaskRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.error("Task execution rejected - thread pool is full and queue is full");
            throw new RejectedExecutionException("Task execution rejected - system overloaded");
        }
    }
    
    /**
     * Statistics interface for the executor
     */
    public interface ExecutorStatistics {
        long getTotalTasksExecuted();
        long getTotalTasksCompleted();
        long getTotalTasksFailed();
        double getAverageExecutionTimeMs();
        int getActiveThreadCount();
        int getPoolSize();
        int getCorePoolSize();
        int getMaximumPoolSize();
        long getCompletedTaskCount();
        int getQueueSize();
    }
}