package com.enterprise.taskexecution.core;

import java.time.Duration;
import java.util.function.Predicate;

/**
 * Configurable retry policy for task execution
 */
public interface RetryPolicy {
    
    /**
     * Determine if a task should be retried based on the result
     */
    boolean shouldRetry(Task task, TaskResult result, int currentRetryCount);
    
    /**
     * Calculate the delay before the next retry attempt
     */
    Duration getRetryDelay(Task task, TaskResult result, int currentRetryCount);
    
    /**
     * Get the maximum number of retry attempts
     */
    int getMaxRetries();
    
    /**
     * Default retry policy implementations
     */
    class DefaultRetryPolicy implements RetryPolicy {
        private final int maxRetries;
        private final Duration baseDelay;
        private final double backoffMultiplier;
        private final Duration maxDelay;
        private final Predicate<Throwable> retryableExceptions;
        
        public DefaultRetryPolicy(int maxRetries, Duration baseDelay, 
                                double backoffMultiplier, Duration maxDelay,
                                Predicate<Throwable> retryableExceptions) {
            this.maxRetries = maxRetries;
            this.baseDelay = baseDelay;
            this.backoffMultiplier = backoffMultiplier;
            this.maxDelay = maxDelay;
            this.retryableExceptions = retryableExceptions;
        }
        
        @Override
        public boolean shouldRetry(Task task, TaskResult result, int currentRetryCount) {
            if (currentRetryCount >= maxRetries) {
                return false;
            }
            
            if (result.isSuccess()) {
                return false;
            }
            
            Throwable exception = result.getException();
            if (exception != null) {
                return retryableExceptions.test(exception);
            }
            
            return result.shouldRetry();
        }
        
        @Override
        public Duration getRetryDelay(Task task, TaskResult result, int currentRetryCount) {
            long delayMs = (long) (baseDelay.toMillis() * Math.pow(backoffMultiplier, currentRetryCount));
            long maxDelayMs = maxDelay.toMillis();
            long actualDelay = Math.min(delayMs, maxDelayMs);
            
            // Add jitter to prevent thundering herd
            long jitter = (long) (actualDelay * 0.1 * Math.random());
            actualDelay += jitter;
            
            return Duration.ofMillis(actualDelay);
        }
        
        @Override
        public int getMaxRetries() {
            return maxRetries;
        }
    }
    
    /**
     * Builder for creating retry policies
     */
    class Builder {
        private int maxRetries = 3;
        private Duration baseDelay = Duration.ofSeconds(5);
        private double backoffMultiplier = 2.0;
        private Duration maxDelay = Duration.ofMinutes(5);
        private Predicate<Throwable> retryableExceptions = ex -> true;
        
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder baseDelay(Duration baseDelay) {
            this.baseDelay = baseDelay;
            return this;
        }
        
        public Builder backoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }
        
        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }
        
        public Builder retryableExceptions(Predicate<Throwable> retryableExceptions) {
            this.retryableExceptions = retryableExceptions;
            return this;
        }
        
        public RetryPolicy build() {
            return new DefaultRetryPolicy(maxRetries, baseDelay, backoffMultiplier, maxDelay, retryableExceptions);
        }
    }
    
    static Builder builder() {
        return new Builder();
    }
    
    /**
     * Predefined retry policies
     */
    class Predefined {
        
        /**
         * No retry policy
         */
        public static RetryPolicy noRetry() {
            return builder().maxRetries(0).build();
        }
        
        /**
         * Quick retry policy for transient failures
         */
        public static RetryPolicy quickRetry() {
            return builder()
                .maxRetries(3)
                .baseDelay(Duration.ofSeconds(1))
                .backoffMultiplier(1.5)
                .maxDelay(Duration.ofSeconds(10))
                .build();
        }
        
        /**
         * Standard retry policy
         */
        public static RetryPolicy standard() {
            return builder()
                .maxRetries(3)
                .baseDelay(Duration.ofSeconds(5))
                .backoffMultiplier(2.0)
                .maxDelay(Duration.ofMinutes(5))
                .build();
        }
        
        /**
         * Aggressive retry policy for critical tasks
         */
        public static RetryPolicy aggressive() {
            return builder()
                .maxRetries(10)
                .baseDelay(Duration.ofSeconds(2))
                .backoffMultiplier(1.5)
                .maxDelay(Duration.ofMinutes(10))
                .build();
        }
        
        /**
         * Retry policy for network-related failures
         */
        public static RetryPolicy networkRetry() {
            return builder()
                .maxRetries(5)
                .baseDelay(Duration.ofSeconds(3))
                .backoffMultiplier(2.0)
                .maxDelay(Duration.ofMinutes(2))
                .retryableExceptions(ex -> 
                    ex instanceof java.net.ConnectException ||
                    ex instanceof java.net.SocketTimeoutException ||
                    ex instanceof java.io.IOException)
                .build();
        }
    }
}