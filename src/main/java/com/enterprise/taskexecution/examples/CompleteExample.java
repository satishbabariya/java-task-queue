package com.enterprise.taskexecution.examples;

import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.core.*;
import com.enterprise.taskexecution.scheduler.CronScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Complete example demonstrating the task execution engine capabilities
 */
public class CompleteExample {
    
    private static final Logger logger = LoggerFactory.getLogger(CompleteExample.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting Task Execution Engine Example");
        
        // Create engine with scheduler
        TaskExecutionEngineFactory.TaskExecutionEngineWithScheduler engineWithScheduler = 
            TaskExecutionEngineFactory.createWithScheduler();
        
        TaskExecutionEngine engine = engineWithScheduler.getEngine();
        CronScheduler scheduler = engineWithScheduler.getScheduler();
        
        // Register task handlers
        engine.registerHandler(new EmailTaskHandler());
        engine.registerHandler(new DataProcessingHandler());
        engine.registerHandler(new ReportGenerationHandler());
        
        // Start the engine
        engineWithScheduler.start();
        logger.info("Task Execution Engine started");
        
        try {
            // Example 1: Submit immediate tasks
            runImmediateTaskExample(engine);
            
            // Example 2: Schedule delayed tasks
            runScheduledTaskExample(engine);
            
            // Example 3: Use cron scheduler
            runCronSchedulerExample(scheduler);
            
            // Example 4: Demonstrate retry mechanisms
            runRetryExample(engine);
            
            // Example 5: Monitor engine statistics
            runMonitoringExample(engine);
            
            // Wait for tasks to complete
            Thread.sleep(10000);
            
        } finally {
            // Stop the engine
            engineWithScheduler.stop();
            logger.info("Task Execution Engine stopped");
        }
    }
    
    private static void runImmediateTaskExample(TaskExecutionEngine engine) throws Exception {
        logger.info("=== Immediate Task Example ===");
        
        // Send an email
        Map<String, Object> emailPayload = Map.of(
            "recipient", "user@example.com",
            "subject", "Welcome to our service",
            "body", "Thank you for signing up!",
            "template", "welcome"
        );
        
        Task emailTask = TaskImpl.builder()
            .type("email")
            .payload(emailPayload)
            .priority(10)
            .timeoutMs(30000)
            .build();
        
        CompletableFuture<TaskResult> emailFuture = engine.submit(emailTask);
        TaskResult emailResult = emailFuture.get(10, TimeUnit.SECONDS);
        
        logger.info("Email task result: {}", emailResult.isSuccess() ? "SUCCESS" : "FAILED");
        if (emailResult.isSuccess()) {
            logger.info("Email sent to: {}", emailResult.getResultData().get("recipient"));
        }
        
        // Process some data
        Map<String, Object> dataPayload = Map.of(
            "dataSource", "user_activity",
            "operation", "aggregate",
            "batchSize", 5000,
            "validateData", true
        );
        
        Task dataTask = TaskImpl.builder()
            .type("data-processing")
            .payload(dataPayload)
            .priority(5)
            .build();
        
        CompletableFuture<TaskResult> dataFuture = engine.submit(dataTask);
        TaskResult dataResult = dataFuture.get(15, TimeUnit.SECONDS);
        
        logger.info("Data processing result: {}", dataResult.isSuccess() ? "SUCCESS" : "FAILED");
        if (dataResult.isSuccess()) {
            logger.info("Records processed: {}", dataResult.getResultData().get("recordsProcessed"));
        }
    }
    
    private static void runScheduledTaskExample(TaskExecutionEngine engine) throws Exception {
        logger.info("=== Scheduled Task Example ===");
        
        // Schedule a report generation for later
        Map<String, Object> reportPayload = Map.of(
            "reportType", "financial",
            "format", "pdf",
            "dateRange", "last30days",
            "includeCharts", true,
            "outputPath", "/reports"
        );
        
        Task reportTask = TaskImpl.builder()
            .type("report-generation")
            .payload(reportPayload)
            .scheduledFor(Instant.now().plusSeconds(2))
            .priority(8)
            .build();
        
        CompletableFuture<Void> reportFuture = engine.schedule(reportTask);
        logger.info("Report generation scheduled for 2 seconds from now");
        
        // Wait for the scheduled task to execute
        reportFuture.get(10, TimeUnit.SECONDS);
        logger.info("Scheduled report generation completed");
    }
    
    private static void runCronSchedulerExample(CronScheduler scheduler) throws Exception {
        logger.info("=== Cron Scheduler Example ===");
        
        // Schedule a daily report generation
        Map<String, Object> dailyReportPayload = Map.of(
            "reportType", "sales",
            "format", "pdf",
            "dateRange", "last24hours",
            "includeCharts", true
        );
        
        scheduler.scheduleCron(
            "daily-sales-report",
            "0 0 8 * * *", // Every day at 8 AM
            "report-generation",
            dailyReportPayload,
            Map.of("priority", "high", "recipients", "sales-team@example.com")
        );
        
        logger.info("Daily sales report scheduled for 8 AM every day");
        
        // Schedule a weekly data cleanup
        Map<String, Object> cleanupPayload = Map.of(
            "dataSource", "temp_files",
            "operation", "cleanup",
            "batchSize", 10000,
            "validateData", false
        );
        
        scheduler.scheduleCron(
            "weekly-cleanup",
            "0 2 * * 0", // Every Sunday at 2 AM
            "data-processing",
            cleanupPayload,
            Map.of("priority", "low")
        );
        
        logger.info("Weekly cleanup scheduled for Sunday at 2 AM");
    }
    
    private static void runRetryExample(TaskExecutionEngine engine) throws Exception {
        logger.info("=== Retry Example ===");
        
        // Create a task that will fail initially but succeed on retry
        Map<String, Object> retryPayload = Map.of(
            "dataSource", "unreliable_service",
            "operation", "fetch",
            "batchSize", 100,
            "validateData", true
        );
        
        Task retryTask = TaskImpl.builder()
            .type("data-processing")
            .payload(retryPayload)
            .maxRetries(3)
            .retryDelayMs(1000)
            .timeoutMs(5000)
            .build();
        
        CompletableFuture<TaskResult> retryFuture = engine.submit(retryTask);
        TaskResult retryResult = retryFuture.get(20, TimeUnit.SECONDS);
        
        logger.info("Retry task result: {}", retryResult.isSuccess() ? "SUCCESS" : "FAILED");
        if (!retryResult.isSuccess()) {
            logger.info("Final error: {}", retryResult.getErrorMessage());
        }
    }
    
    private static void runMonitoringExample(TaskExecutionEngine engine) throws Exception {
        logger.info("=== Monitoring Example ===");
        
        // Get engine statistics
        EngineStatistics stats = engine.getStatistics();
        
        logger.info("Engine Statistics:");
        logger.info("  Total tasks submitted: {}", stats.getTotalTasksSubmitted());
        logger.info("  Total tasks completed: {}", stats.getTotalTasksCompleted());
        logger.info("  Total tasks failed: {}", stats.getTotalTasksFailed());
        logger.info("  Tasks running: {}", stats.getTasksRunning());
        logger.info("  Tasks pending: {}", stats.getTasksPending());
        logger.info("  Queue size: {}", stats.getQueueSize());
        logger.info("  Average execution time: {:.2f}ms", stats.getAverageExecutionTimeMs());
        logger.info("  Uptime: {}ms", stats.getUptimeMs());
        
        // Get task counts by status
        logger.info("Task counts by status:");
        stats.getTaskCountsByStatus().forEach((status, count) -> {
            logger.info("  {}: {}", status, count);
        });
        
        // Get task counts by type
        logger.info("Task counts by type:");
        stats.getTaskCountsByType().forEach((type, count) -> {
            logger.info("  {}: {}", type, count);
        });
    }
}