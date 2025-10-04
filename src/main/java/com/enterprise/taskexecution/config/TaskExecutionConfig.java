package com.enterprise.taskexecution.config;

import java.time.Duration;
import java.util.Map;

/**
 * Configuration for the task execution engine
 */
public class TaskExecutionConfig {
    
    private final ExecutorConfig executorConfig;
    private final QueueConfig queueConfig;
    private final RetryConfig retryConfig;
    private final MonitoringConfig monitoringConfig;
    private final DeadLetterQueueConfig dlqConfig;
    private final Map<String, Object> customProperties;
    
    public TaskExecutionConfig(ExecutorConfig executorConfig, QueueConfig queueConfig, 
                             RetryConfig retryConfig, MonitoringConfig monitoringConfig,
                             DeadLetterQueueConfig dlqConfig, Map<String, Object> customProperties) {
        this.executorConfig = executorConfig;
        this.queueConfig = queueConfig;
        this.retryConfig = retryConfig;
        this.monitoringConfig = monitoringConfig;
        this.dlqConfig = dlqConfig;
        this.customProperties = customProperties;
    }
    
    public ExecutorConfig getExecutorConfig() { return executorConfig; }
    public QueueConfig getQueueConfig() { return queueConfig; }
    public RetryConfig getRetryConfig() { return retryConfig; }
    public MonitoringConfig getMonitoringConfig() { return monitoringConfig; }
    public DeadLetterQueueConfig getDlqConfig() { return dlqConfig; }
    public Map<String, Object> getCustomProperties() { return customProperties; }
    
    /**
     * Executor configuration
     */
    public static class ExecutorConfig {
        private final int corePoolSize;
        private final int maximumPoolSize;
        private final Duration keepAliveTime;
        private final int queueCapacity;
        private final Duration shutdownTimeout;
        
        public ExecutorConfig(int corePoolSize, int maximumPoolSize, Duration keepAliveTime, 
                            int queueCapacity, Duration shutdownTimeout) {
            this.corePoolSize = corePoolSize;
            this.maximumPoolSize = maximumPoolSize;
            this.keepAliveTime = keepAliveTime;
            this.queueCapacity = queueCapacity;
            this.shutdownTimeout = shutdownTimeout;
        }
        
        public int getCorePoolSize() { return corePoolSize; }
        public int getMaximumPoolSize() { return maximumPoolSize; }
        public Duration getKeepAliveTime() { return keepAliveTime; }
        public int getQueueCapacity() { return queueCapacity; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
    }
    
    /**
     * Queue configuration
     */
    public static class QueueConfig {
        private final String dbPath;
        private final boolean enableCompression;
        private final int maxQueueSize;
        private final Duration cleanupInterval;
        
        public QueueConfig(String dbPath, boolean enableCompression, int maxQueueSize, 
                          Duration cleanupInterval) {
            this.dbPath = dbPath;
            this.enableCompression = enableCompression;
            this.maxQueueSize = maxQueueSize;
            this.cleanupInterval = cleanupInterval;
        }
        
        public String getDbPath() { return dbPath; }
        public boolean isEnableCompression() { return enableCompression; }
        public int getMaxQueueSize() { return maxQueueSize; }
        public Duration getCleanupInterval() { return cleanupInterval; }
    }
    
    /**
     * Retry configuration
     */
    public static class RetryConfig {
        private final int maxRetries;
        private final Duration baseDelay;
        private final double backoffMultiplier;
        private final Duration maxDelay;
        private final boolean enableJitter;
        
        public RetryConfig(int maxRetries, Duration baseDelay, double backoffMultiplier, 
                          Duration maxDelay, boolean enableJitter) {
            this.maxRetries = maxRetries;
            this.baseDelay = baseDelay;
            this.backoffMultiplier = backoffMultiplier;
            this.maxDelay = maxDelay;
            this.enableJitter = enableJitter;
        }
        
        public int getMaxRetries() { return maxRetries; }
        public Duration getBaseDelay() { return baseDelay; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public Duration getMaxDelay() { return maxDelay; }
        public boolean isEnableJitter() { return enableJitter; }
    }
    
    /**
     * Monitoring configuration
     */
    public static class MonitoringConfig {
        private final boolean enableMetrics;
        private final boolean enableHealthChecks;
        private final Duration healthCheckInterval;
        private final boolean enableTaskTracing;
        
        public MonitoringConfig(boolean enableMetrics, boolean enableHealthChecks, 
                              Duration healthCheckInterval, boolean enableTaskTracing) {
            this.enableMetrics = enableMetrics;
            this.enableHealthChecks = enableHealthChecks;
            this.healthCheckInterval = healthCheckInterval;
            this.enableTaskTracing = enableTaskTracing;
        }
        
        public boolean isEnableMetrics() { return enableMetrics; }
        public boolean isEnableHealthChecks() { return enableHealthChecks; }
        public Duration getHealthCheckInterval() { return healthCheckInterval; }
        public boolean isEnableTaskTracing() { return enableTaskTracing; }
    }
    
    /**
     * Dead Letter Queue configuration
     */
    public static class DeadLetterQueueConfig {
        private final boolean enabled;
        private final String dbPath;
        private final int maxCapacity;
        private final boolean enableRetentionPolicy;
        private final long retentionDays;
        private final Duration cleanupInterval;
        
        public DeadLetterQueueConfig(boolean enabled, String dbPath, int maxCapacity,
                                   boolean enableRetentionPolicy, long retentionDays,
                                   Duration cleanupInterval) {
            this.enabled = enabled;
            this.dbPath = dbPath;
            this.maxCapacity = maxCapacity;
            this.enableRetentionPolicy = enableRetentionPolicy;
            this.retentionDays = retentionDays;
            this.cleanupInterval = cleanupInterval;
        }
        
        public boolean isEnabled() { return enabled; }
        public String getDbPath() { return dbPath; }
        public int getMaxCapacity() { return maxCapacity; }
        public boolean isEnableRetentionPolicy() { return enableRetentionPolicy; }
        public long getRetentionDays() { return retentionDays; }
        public Duration getCleanupInterval() { return cleanupInterval; }
    }
    
    /**
     * Builder for creating configurations
     */
    public static class Builder {
        private ExecutorConfig executorConfig = Defaults.defaultExecutorConfig();
        private QueueConfig queueConfig = Defaults.defaultQueueConfig();
        private RetryConfig retryConfig = Defaults.defaultRetryConfig();
        private MonitoringConfig monitoringConfig = Defaults.defaultMonitoringConfig();
        private DeadLetterQueueConfig dlqConfig = Defaults.defaultDeadLetterQueueConfig();
        private Map<String, Object> customProperties = new java.util.HashMap<>();
        
        public Builder executorConfig(ExecutorConfig executorConfig) {
            this.executorConfig = executorConfig;
            return this;
        }
        
        public Builder queueConfig(QueueConfig queueConfig) {
            this.queueConfig = queueConfig;
            return this;
        }
        
        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }
        
        public Builder monitoringConfig(MonitoringConfig monitoringConfig) {
            this.monitoringConfig = monitoringConfig;
            return this;
        }
        
        public Builder dlqConfig(DeadLetterQueueConfig dlqConfig) {
            this.dlqConfig = dlqConfig;
            return this;
        }
        
        public Builder customProperty(String key, Object value) {
            this.customProperties.put(key, value);
            return this;
        }
        
        public Builder customProperties(Map<String, Object> properties) {
            this.customProperties.putAll(properties);
            return this;
        }
        
        public TaskExecutionConfig build() {
            return new TaskExecutionConfig(executorConfig, queueConfig, retryConfig, 
                                        monitoringConfig, dlqConfig, customProperties);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Default configurations
     */
    public static class Defaults {
        public static ExecutorConfig defaultExecutorConfig() {
            return new ExecutorConfig(
                4, 16, Duration.ofMinutes(1), 1000, Duration.ofSeconds(30)
            );
        }
        
        public static QueueConfig defaultQueueConfig() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            String uniqueName = java.util.UUID.randomUUID().toString();
            String path = tmpDir.endsWith("/") ? (tmpDir + "task-queue-" + uniqueName + ".db")
                                                : (tmpDir + "/task-queue-" + uniqueName + ".db");
            return new QueueConfig(
                path, true, 100000, Duration.ofHours(1)
            );
        }
        
        public static RetryConfig defaultRetryConfig() {
            return new RetryConfig(
                3, Duration.ofSeconds(5), 2.0, Duration.ofMinutes(5), true
            );
        }
        
        public static MonitoringConfig defaultMonitoringConfig() {
            return new MonitoringConfig(
                true, true, Duration.ofMinutes(1), false
            );
        }
        
        public static DeadLetterQueueConfig defaultDeadLetterQueueConfig() {
            String tmpDir = System.getProperty("java.io.tmpdir");
            String uniqueName = java.util.UUID.randomUUID().toString();
            String path = tmpDir.endsWith("/") ? (tmpDir + "dlq-" + uniqueName + ".db")
                                              : (tmpDir + "/dlq-" + uniqueName + ".db");
            return new DeadLetterQueueConfig(
                true, path, 10000, true, 30, Duration.ofHours(6)
            );
        }
    }
}