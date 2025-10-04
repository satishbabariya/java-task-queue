package com.enterprise.taskexecution.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates task execution configuration
 */
public class ConfigValidator {
    
    /**
     * Validate the configuration and return any validation errors
     */
    public List<ValidationError> validate(TaskExecutionConfig config) {
        List<ValidationError> errors = new ArrayList<>();
        
        // Validate executor config
        validateExecutorConfig(config.getExecutorConfig(), errors);
        
        // Validate queue config
        validateQueueConfig(config.getQueueConfig(), errors);
        
        // Validate retry config
        validateRetryConfig(config.getRetryConfig(), errors);
        
        // Validate monitoring config
        validateMonitoringConfig(config.getMonitoringConfig(), errors);
        
        return errors;
    }
    
    private void validateExecutorConfig(TaskExecutionConfig.ExecutorConfig config, List<ValidationError> errors) {
        if (config.getCorePoolSize() <= 0) {
            errors.add(new ValidationError("executor.corePoolSize", 
                "Core pool size must be greater than 0"));
        }
        
        if (config.getMaximumPoolSize() <= 0) {
            errors.add(new ValidationError("executor.maximumPoolSize", 
                "Maximum pool size must be greater than 0"));
        }
        
        if (config.getCorePoolSize() > config.getMaximumPoolSize()) {
            errors.add(new ValidationError("executor.poolSize", 
                "Core pool size cannot be greater than maximum pool size"));
        }
        
        if (config.getKeepAliveTime().isNegative()) {
            errors.add(new ValidationError("executor.keepAliveTime", 
                "Keep alive time cannot be negative"));
        }
        
        if (config.getQueueCapacity() <= 0) {
            errors.add(new ValidationError("executor.queueCapacity", 
                "Queue capacity must be greater than 0"));
        }
        
        if (config.getShutdownTimeout().isNegative()) {
            errors.add(new ValidationError("executor.shutdownTimeout", 
                "Shutdown timeout cannot be negative"));
        }
    }
    
    private void validateQueueConfig(TaskExecutionConfig.QueueConfig config, List<ValidationError> errors) {
        if (config.getDbPath() == null || config.getDbPath().trim().isEmpty()) {
            errors.add(new ValidationError("queue.dbPath", 
                "Database path is required"));
        }
        
        if (config.getMaxQueueSize() <= 0) {
            errors.add(new ValidationError("queue.maxQueueSize", 
                "Maximum queue size must be greater than 0"));
        }
        
        if (config.getCleanupInterval().isNegative()) {
            errors.add(new ValidationError("queue.cleanupInterval", 
                "Cleanup interval cannot be negative"));
        }
    }
    
    private void validateRetryConfig(TaskExecutionConfig.RetryConfig config, List<ValidationError> errors) {
        if (config.getMaxRetries() < 0) {
            errors.add(new ValidationError("retry.maxRetries", 
                "Maximum retries cannot be negative"));
        }
        
        if (config.getBaseDelay().isNegative()) {
            errors.add(new ValidationError("retry.baseDelay", 
                "Base delay cannot be negative"));
        }
        
        if (config.getBackoffMultiplier() <= 0) {
            errors.add(new ValidationError("retry.backoffMultiplier", 
                "Backoff multiplier must be greater than 0"));
        }
        
        if (config.getMaxDelay().isNegative()) {
            errors.add(new ValidationError("retry.maxDelay", 
                "Maximum delay cannot be negative"));
        }
        
        if (config.getBaseDelay().compareTo(config.getMaxDelay()) > 0) {
            errors.add(new ValidationError("retry.delayRange", 
                "Base delay cannot be greater than maximum delay"));
        }
    }
    
    private void validateMonitoringConfig(TaskExecutionConfig.MonitoringConfig config, List<ValidationError> errors) {
        if (config.getHealthCheckInterval().isNegative()) {
            errors.add(new ValidationError("monitoring.healthCheckInterval", 
                "Health check interval cannot be negative"));
        }
    }
    
    /**
     * Validation error
     */
    public static class ValidationError {
        private final String field;
        private final String message;
        
        public ValidationError(String field, String message) {
            this.field = field;
            this.message = message;
        }
        
        public String getField() { return field; }
        public String getMessage() { return message; }
        
        @Override
        public String toString() {
            return String.format("%s: %s", field, message);
        }
    }
}