package com.enterprise.taskexecution.monitoring;

import com.enterprise.taskexecution.core.TaskExecutionEngine;
import com.enterprise.taskexecution.queue.PersistentTaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Health checker for the task execution engine
 */
public class HealthChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthChecker.class);
    
    private final TaskExecutionEngine engine;
    private final PersistentTaskQueue queue;
    private final MetricsCollector metricsCollector;
    
    public HealthChecker(TaskExecutionEngine engine, PersistentTaskQueue queue, 
                        MetricsCollector metricsCollector) {
        this.engine = engine;
        this.queue = queue;
        this.metricsCollector = metricsCollector;
    }
    
    /**
     * Perform a comprehensive health check
     */
    public CompletableFuture<HealthStatus> performHealthCheck() {
        return CompletableFuture.supplyAsync(() -> {
            HealthStatus.Builder builder = HealthStatus.builder();
            
            // Check engine status
            checkEngineStatus(builder);
            
            // Check queue health
            checkQueueHealth(builder);
            
            // Check system resources
            checkSystemResources(builder);
            
            // Check task execution
            checkTaskExecution(builder);
            
            return builder.build();
        });
    }
    
    private void checkEngineStatus(HealthStatus.Builder builder) {
        try {
            boolean isRunning = engine.isRunning();
            builder.addCheck("engine.running", isRunning, 
                isRunning ? "Engine is running" : "Engine is not running");
            
            if (isRunning) {
                com.enterprise.taskexecution.core.EngineStatistics stats = engine.getStatistics();
                long uptimeMs = stats.getUptimeMs();
                builder.addCheck("engine.uptime", uptimeMs > 0, 
                    String.format("Engine uptime: %dms", uptimeMs));
            }
            
        } catch (Exception e) {
            builder.addCheck("engine.status", false, "Error checking engine status: " + e.getMessage());
        }
    }
    
    private void checkQueueHealth(HealthStatus.Builder builder) {
        try {
            int queueSize = queue.size();
            builder.addCheck("queue.size", queueSize >= 0, 
                String.format("Queue size: %d", queueSize));
            
            // Check if queue is not too large
            boolean queueHealthy = queueSize < 10000; // Configurable threshold
            builder.addCheck("queue.healthy", queueHealthy, 
                String.format("Queue size is %s", queueHealthy ? "healthy" : "too large"));
            
        } catch (Exception e) {
            builder.addCheck("queue.status", false, "Error checking queue status: " + e.getMessage());
        }
    }
    
    private void checkSystemResources(HealthStatus.Builder builder) {
        try {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            double memoryUsagePercent = (double) usedMemory / totalMemory * 100;
            
            boolean memoryHealthy = memoryUsagePercent < 90; // 90% threshold
            builder.addCheck("system.memory", memoryHealthy, 
                String.format("Memory usage: %.2f%% (%d/%d MB)", 
                    memoryUsagePercent, usedMemory / 1024 / 1024, totalMemory / 1024 / 1024));
            
            // Check available processors
            int processors = runtime.availableProcessors();
            builder.addCheck("system.processors", processors > 0, 
                String.format("Available processors: %d", processors));
            
        } catch (Exception e) {
            builder.addCheck("system.resources", false, "Error checking system resources: " + e.getMessage());
        }
    }
    
    private void checkTaskExecution(HealthStatus.Builder builder) {
        try {
            com.enterprise.taskexecution.core.EngineStatistics stats = engine.getStatistics();
            
            long totalSubmitted = stats.getTotalTasksSubmitted();
            long totalCompleted = stats.getTotalTasksCompleted();
            long totalFailed = stats.getTotalTasksFailed();
            
            if (totalSubmitted > 0) {
                double successRate = (double) totalCompleted / totalSubmitted * 100;
                boolean successRateHealthy = successRate > 80; // 80% threshold
                
                builder.addCheck("tasks.success_rate", successRateHealthy, 
                    String.format("Task success rate: %.2f%% (%d/%d)", 
                        successRate, totalCompleted, totalSubmitted));
            }
            
            long runningTasks = stats.getTasksRunning();
            builder.addCheck("tasks.running", runningTasks >= 0, 
                String.format("Running tasks: %d", runningTasks));
            
            double avgExecutionTime = stats.getAverageExecutionTimeMs();
            boolean executionTimeHealthy = avgExecutionTime < 30000; // 30 seconds threshold
            builder.addCheck("tasks.execution_time", executionTimeHealthy, 
                String.format("Average execution time: %.2fms", avgExecutionTime));
            
        } catch (Exception e) {
            builder.addCheck("tasks.status", false, "Error checking task execution: " + e.getMessage());
        }
    }
    
    /**
     * Health status result
     */
    public static class HealthStatus {
        private final boolean healthy;
        private final Map<String, CheckResult> checks;
        private final Instant timestamp;
        
        private HealthStatus(boolean healthy, Map<String, CheckResult> checks, Instant timestamp) {
            this.healthy = healthy;
            this.checks = checks;
            this.timestamp = timestamp;
        }
        
        public boolean isHealthy() { return healthy; }
        public Map<String, CheckResult> getChecks() { return checks; }
        public Instant getTimestamp() { return timestamp; }
        
        public static class CheckResult {
            private final boolean passed;
            private final String message;
            
            public CheckResult(boolean passed, String message) {
                this.passed = passed;
                this.message = message;
            }
            
            public boolean isPassed() { return passed; }
            public String getMessage() { return message; }
        }
        
        public static class Builder {
            private final Map<String, CheckResult> checks = new java.util.concurrent.ConcurrentHashMap<>();
            
            public Builder addCheck(String name, boolean passed, String message) {
                checks.put(name, new CheckResult(passed, message));
                return this;
            }
            
            public HealthStatus build() {
                boolean healthy = checks.values().stream().allMatch(CheckResult::isPassed);
                return new HealthStatus(healthy, checks, Instant.now());
            }
        }
        
        public static Builder builder() {
            return new Builder();
        }
    }
}