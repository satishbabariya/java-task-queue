package com.enterprise.taskexecution.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Handles task execution failures and retries
 */
public class FailureHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(FailureHandler.class);
    
    private final RetryPolicy retryPolicy;
    private final Consumer<Task> deadLetterHandler;
    private final Consumer<Task> failureNotifier;
    
    public FailureHandler(RetryPolicy retryPolicy, Consumer<Task> deadLetterHandler, 
                         Consumer<Task> failureNotifier) {
        this.retryPolicy = retryPolicy;
        this.deadLetterHandler = deadLetterHandler;
        this.failureNotifier = failureNotifier;
    }
    
    /**
     * Handle a failed task execution
     */
    public CompletableFuture<Void> handleFailure(Task task, TaskResult result) {
        return CompletableFuture.runAsync(() -> {
            int currentRetryCount = task.getRetryCount();
            
            if (retryPolicy.shouldRetry(task, result, currentRetryCount)) {
                handleRetry(task, result, currentRetryCount);
            } else {
                handleFinalFailure(task, result);
            }
        });
    }
    
    private void handleRetry(Task task, TaskResult result, int currentRetryCount) {
        Duration retryDelay = retryPolicy.getRetryDelay(task, result, currentRetryCount);
        
        logger.warn("Task {} failed (attempt {}/{}), retrying in {}ms. Error: {}", 
                   task.getId(), currentRetryCount + 1, retryPolicy.getMaxRetries(),
                   retryDelay.toMillis(), result.getErrorMessage());
        
        // Notify about retry
        if (failureNotifier != null) {
            try {
                failureNotifier.accept(task);
            } catch (Exception e) {
                logger.error("Error in failure notifier", e);
            }
        }
        
        // Schedule retry
        Task retryTask = task.withRetryCount(currentRetryCount + 1);
        scheduleRetry(retryTask, retryDelay);
    }
    
    private void handleFinalFailure(Task task, TaskResult result) {
        logger.error("Task {} failed permanently after {} retries. Final error: {}", 
                   task.getId(), task.getRetryCount(), result.getErrorMessage());
        
        // Send to dead letter queue
        if (deadLetterHandler != null) {
            try {
                deadLetterHandler.accept(task);
            } catch (Exception e) {
                logger.error("Error in dead letter handler", e);
            }
        }
        
        // Notify about final failure
        if (failureNotifier != null) {
            try {
                failureNotifier.accept(task);
            } catch (Exception e) {
                logger.error("Error in failure notifier", e);
            }
        }
    }
    
    private void scheduleRetry(Task task, Duration delay) {
        // This would typically integrate with the task scheduler
        // For now, we'll just log the retry
        logger.info("Scheduling retry for task {} in {}ms", task.getId(), delay.toMillis());
    }
    
    /**
     * Builder for creating failure handlers
     */
    public static class Builder {
        private RetryPolicy retryPolicy = RetryPolicy.Predefined.standard();
        private Consumer<Task> deadLetterHandler;
        private Consumer<Task> failureNotifier;
        
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }
        
        public Builder deadLetterHandler(Consumer<Task> deadLetterHandler) {
            this.deadLetterHandler = deadLetterHandler;
            return this;
        }
        
        public Builder failureNotifier(Consumer<Task> failureNotifier) {
            this.failureNotifier = failureNotifier;
            return this;
        }
        
        public FailureHandler build() {
            return new FailureHandler(retryPolicy, deadLetterHandler, failureNotifier);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
}