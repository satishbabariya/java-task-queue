package com.enterprise.taskexecution;

import com.enterprise.taskexecution.config.TaskExecutionConfig;
import com.enterprise.taskexecution.config.ConfigValidator;
import com.enterprise.taskexecution.core.*;
import com.enterprise.taskexecution.dlq.DeadLetterQueue;
import com.enterprise.taskexecution.dlq.MapDBDeadLetterQueue;
import com.enterprise.taskexecution.monitoring.MetricsCollector;
import com.enterprise.taskexecution.monitoring.HealthChecker;
import com.enterprise.taskexecution.queue.PersistentTaskQueue;
import com.enterprise.taskexecution.scheduler.CronScheduler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory for creating and configuring the task execution engine
 */
public class TaskExecutionEngineFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutionEngineFactory.class);
    
    /**
     * Create a task execution engine with default configuration
     */
    public static TaskExecutionEngine createDefault() {
        return create(TaskExecutionConfig.builder().build());
    }
    
    /**
     * Create a task execution engine with custom configuration
     */
    public static TaskExecutionEngine create(TaskExecutionConfig config) {
        // Validate configuration
        ConfigValidator validator = new ConfigValidator();
        List<ConfigValidator.ValidationError> errors = validator.validate(config);
        
        if (!errors.isEmpty()) {
            StringBuilder errorMsg = new StringBuilder("Configuration validation failed:\n");
            errors.forEach(error -> errorMsg.append("  - ").append(error).append("\n"));
            throw new IllegalArgumentException(errorMsg.toString());
        }
        
        logger.info("Creating TaskExecutionEngine with configuration: {}", config);
        
        try {
            // Create components
            PersistentTaskQueue queue = createQueue(config.getQueueConfig());
            AsyncTaskExecutor executor = createExecutor(config.getExecutorConfig());
            DeadLetterQueue deadLetterQueue = createDeadLetterQueue(config.getDlqConfig());
            TaskExecutionEngine engine = new TaskExecutionEngineImpl(queue, executor, deadLetterQueue);
            
            // Setup monitoring if enabled
            if (config.getMonitoringConfig().isEnableMetrics()) {
                MeterRegistry meterRegistry = new SimpleMeterRegistry();
                MetricsCollector metricsCollector = new MetricsCollector(meterRegistry);
                // TODO: Integrate metrics collector with engine
            }
            
            logger.info("TaskExecutionEngine created successfully");
            return engine;
            
        } catch (Exception e) {
            logger.error("Failed to create TaskExecutionEngine", e);
            throw new RuntimeException("Failed to create TaskExecutionEngine", e);
        }
    }
    
    /**
     * Create a task execution engine with cron scheduler
     */
    public static TaskExecutionEngineWithScheduler createWithScheduler(TaskExecutionConfig config) {
        TaskExecutionEngine engine = create(config);
        CronScheduler scheduler = new CronScheduler(engine);
        
        return new TaskExecutionEngineWithScheduler(engine, scheduler);
    }
    
    /**
     * Create a task execution engine with cron scheduler and default config
     */
    public static TaskExecutionEngineWithScheduler createWithScheduler() {
        return createWithScheduler(TaskExecutionConfig.builder().build());
    }
    
    private static PersistentTaskQueue createQueue(TaskExecutionConfig.QueueConfig config) {
        return new PersistentTaskQueue(config.getDbPath());
    }
    
    private static AsyncTaskExecutor createExecutor(TaskExecutionConfig.ExecutorConfig config) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(config.getQueueCapacity());
        
        return new AsyncTaskExecutor(
            config.getCorePoolSize(),
            config.getMaximumPoolSize(),
            config.getKeepAliveTime().toMillis(),
            java.util.concurrent.TimeUnit.MILLISECONDS,
            workQueue
        );
    }
    
    private static DeadLetterQueue createDeadLetterQueue(TaskExecutionConfig.DeadLetterQueueConfig config) {
        if (!config.isEnabled()) {
            return null;
        }
        
        return new MapDBDeadLetterQueue(
            config.getDbPath(),
            config.getMaxCapacity(),
            config.isEnableRetentionPolicy(),
            config.getRetentionDays()
        );
    }
    
    /**
     * Wrapper class that includes both engine and scheduler
     */
    public static class TaskExecutionEngineWithScheduler {
        private final TaskExecutionEngine engine;
        private final CronScheduler scheduler;
        
        public TaskExecutionEngineWithScheduler(TaskExecutionEngine engine, CronScheduler scheduler) {
            this.engine = engine;
            this.scheduler = scheduler;
        }
        
        public TaskExecutionEngine getEngine() { return engine; }
        public CronScheduler getScheduler() { return scheduler; }
        
        public void start() {
            engine.start();
            scheduler.start();
        }
        
        public void stop() {
            scheduler.stop();
            engine.stop();
        }
        
        public boolean isRunning() {
            return engine.isRunning() && scheduler.isRunning();
        }
    }
}