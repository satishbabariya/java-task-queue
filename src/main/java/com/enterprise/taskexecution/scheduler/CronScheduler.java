package com.enterprise.taskexecution.scheduler;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskExecutionEngine;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Cron-based job scheduler that integrates with the TaskExecutionEngine
 */
public class CronScheduler {
    
    private static final Logger logger = LoggerFactory.getLogger(CronScheduler.class);
    
    private final TaskExecutionEngine engine;
    private final Scheduler cronScheduler;
    private final Map<String, ScheduledJob> scheduledJobs = new ConcurrentHashMap<>();
    
    public CronScheduler(TaskExecutionEngine engine) {
        this.engine = engine;
        this.cronScheduler = new Scheduler();
    }
    
    /**
     * Schedule a task to run at regular intervals using cron expression
     */
    public CompletableFuture<String> scheduleCron(String jobId, String cronExpression, 
                                                String taskType, Map<String, Object> payload,
                                                Map<String, Object> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Validate cron expression
                new SchedulingPattern(cronExpression);
                
                ScheduledJob job = new ScheduledJob(jobId, cronExpression, taskType, payload, metadata);
                scheduledJobs.put(jobId, job);
                
                // Schedule with cron4j
                cronScheduler.schedule(cronExpression, () -> {
                    try {
                        Task task = com.enterprise.taskexecution.core.TaskImpl.builder()
                            .type(taskType)
                            .payload(payload)
                            .metadata(metadata)
                            .build();
                        
                        engine.submit(task);
                        logger.debug("Scheduled task executed for job: {}", jobId);
                        
                    } catch (Exception e) {
                        logger.error("Error executing scheduled task for job: {}", jobId, e);
                    }
                });
                
                logger.info("Scheduled cron job {} with expression: {}", jobId, cronExpression);
                return jobId;
                
            } catch (IllegalArgumentException e) {
                logger.error("Invalid cron expression: {}", cronExpression, e);
                throw new IllegalArgumentException("Invalid cron expression: " + cronExpression, e);
            }
        });
    }
    
    /**
     * Schedule a task to run at a specific time
     */
    public CompletableFuture<String> scheduleAt(String jobId, Instant scheduledTime,
                                              String taskType, Map<String, Object> payload,
                                              Map<String, Object> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            ScheduledJob job = new ScheduledJob(jobId, null, taskType, payload, metadata, scheduledTime);
            scheduledJobs.put(jobId, job);
            
            // Calculate delay
            long delayMs = scheduledTime.toEpochMilli() - Instant.now().toEpochMilli();
            
            if (delayMs > 0) {
                // cron4j does not support delay scheduling directly; use a temporary Scheduler task
                String pattern = "@every " + Math.max(1, delayMs / 1000) + "s";
                String[] jobHolder = new String[1];
                jobHolder[0] = cronScheduler.schedule(pattern, () -> {
                    try {
                        Task task = com.enterprise.taskexecution.core.TaskImpl.builder()
                            .type(taskType)
                            .payload(payload)
                            .metadata(metadata)
                            .build();
                        
                        engine.submit(task);
                        logger.debug("Scheduled task executed for job: {}", jobId);
                        // Deschedule after first run
                        if (jobHolder[0] != null) {
                            cronScheduler.deschedule(jobHolder[0]);
                        }
                    } catch (Exception e) {
                        logger.error("Error executing scheduled task for job: {}", jobId, e);
                    }
                });
                
                logger.info("Scheduled job {} to run at: {}", jobId, scheduledTime);
            } else {
                logger.warn("Scheduled time {} is in the past for job {}", scheduledTime, jobId);
            }
            
            return jobId;
        });
    }
    
    /**
     * Schedule a task to run after a delay
     */
    public CompletableFuture<String> scheduleAfter(String jobId, long delayMs,
                                                 String taskType, Map<String, Object> payload,
                                                 Map<String, Object> metadata) {
        return CompletableFuture.supplyAsync(() -> {
            Instant scheduledTime = Instant.now().plusMillis(delayMs);
            ScheduledJob job = new ScheduledJob(jobId, null, taskType, payload, metadata, scheduledTime);
            scheduledJobs.put(jobId, job);
            
            String pattern = "@every " + Math.max(1, delayMs / 1000) + "s";
            String[] jobHolder = new String[1];
            jobHolder[0] = cronScheduler.schedule(pattern, () -> {
                try {
                    Task task = com.enterprise.taskexecution.core.TaskImpl.builder()
                        .type(taskType)
                        .payload(payload)
                        .metadata(metadata)
                        .build();
                    
                    engine.submit(task);
                    logger.debug("Scheduled task executed for job: {}", jobId);
                    if (jobHolder[0] != null) {
                        cronScheduler.deschedule(jobHolder[0]);
                    }
                } catch (Exception e) {
                    logger.error("Error executing scheduled task for job: {}", jobId, e);
                }
            });
            
            logger.info("Scheduled job {} to run after {}ms", jobId, delayMs);
            return jobId;
        });
    }
    
    /**
     * Cancel a scheduled job
     */
    public CompletableFuture<Boolean> cancelJob(String jobId) {
        return CompletableFuture.supplyAsync(() -> {
            ScheduledJob job = scheduledJobs.remove(jobId);
            if (job != null) {
                cronScheduler.deschedule(jobId);
                logger.info("Cancelled scheduled job: {}", jobId);
                return true;
            }
            return false;
        });
    }
    
    /**
     * Get all scheduled jobs
     */
    public Map<String, ScheduledJob> getScheduledJobs() {
        return new ConcurrentHashMap<>(scheduledJobs);
    }
    
    /**
     * Get a specific scheduled job
     */
    public ScheduledJob getScheduledJob(String jobId) {
        return scheduledJobs.get(jobId);
    }
    
    /**
     * Start the cron scheduler
     */
    public void start() {
        cronScheduler.start();
        logger.info("CronScheduler started");
    }
    
    /**
     * Stop the cron scheduler
     */
    public void stop() {
        cronScheduler.stop();
        scheduledJobs.clear();
        logger.info("CronScheduler stopped");
    }
    
    /**
     * Check if scheduler is running
     */
    public boolean isRunning() {
        return cronScheduler.isStarted();
    }
    
    /**
     * Represents a scheduled job
     */
    public static class ScheduledJob {
        private final String jobId;
        private final String cronExpression;
        private final String taskType;
        private final Map<String, Object> payload;
        private final Map<String, Object> metadata;
        private final Instant scheduledTime;
        private final Instant createdAt;
        
        public ScheduledJob(String jobId, String cronExpression, String taskType, 
                          Map<String, Object> payload, Map<String, Object> metadata) {
            this(jobId, cronExpression, taskType, payload, metadata, null);
        }
        
        public ScheduledJob(String jobId, String cronExpression, String taskType, 
                          Map<String, Object> payload, Map<String, Object> metadata, 
                          Instant scheduledTime) {
            this.jobId = jobId;
            this.cronExpression = cronExpression;
            this.taskType = taskType;
            this.payload = payload;
            this.metadata = metadata;
            this.scheduledTime = scheduledTime;
            this.createdAt = Instant.now();
        }
        
        public String getJobId() { return jobId; }
        public String getCronExpression() { return cronExpression; }
        public String getTaskType() { return taskType; }
        public Map<String, Object> getPayload() { return payload; }
        public Map<String, Object> getMetadata() { return metadata; }
        public Instant getScheduledTime() { return scheduledTime; }
        public Instant getCreatedAt() { return createdAt; }
        
        public boolean isCronJob() {
            return cronExpression != null;
        }
        
        public boolean isOneTimeJob() {
            return scheduledTime != null;
        }
    }
}