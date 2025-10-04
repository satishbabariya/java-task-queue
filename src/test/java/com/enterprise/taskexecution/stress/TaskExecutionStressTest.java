package com.enterprise.taskexecution.stress;

import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.config.TaskExecutionConfig;
import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskHandler;
import com.enterprise.taskexecution.core.TaskResult;
import com.enterprise.taskexecution.core.TaskStatus;
import com.enterprise.taskexecution.core.TaskImpl;
import com.enterprise.taskexecution.core.EngineStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for the Task Execution Engine to validate performance and stability
 * under heavy concurrent load.
 */
public class TaskExecutionStressTest {

    private com.enterprise.taskexecution.core.TaskExecutionEngine engine;
    private ExecutorService testExecutor;
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    private final AtomicLong totalExecutionTime = new AtomicLong(0);
    private final AtomicInteger retryCount = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        // Configure engine for stress testing
        TaskExecutionConfig config = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                20, 50, java.time.Duration.ofMillis(30000), 1000, java.time.Duration.ofSeconds(30)
            ))
            .queueConfig(new TaskExecutionConfig.QueueConfig(
                "/tmp/stress-test-" + UUID.randomUUID() + ".db", true, 10000, java.time.Duration.ofMinutes(5)
            ))
            .retryConfig(new TaskExecutionConfig.RetryConfig(
                3, java.time.Duration.ofMillis(100), 2.0, java.time.Duration.ofMillis(1000), true
            ))
            .build();

        engine = TaskExecutionEngineFactory.create(config);
        testExecutor = Executors.newFixedThreadPool(50);
        
        // Register stress test handlers
        engine.registerHandler(new FastTaskHandler());
        engine.registerHandler(new SlowTaskHandler());
        engine.registerHandler(new FailingTaskHandler());
        engine.registerHandler(new RetryTaskHandler());
        
        // Start the engine
        engine.start();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
        if (testExecutor != null) {
            testExecutor.shutdown();
            try {
                if (!testExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    testExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Timeout(300) // 5 minutes timeout
    void testHighConcurrencyTaskExecution() throws Exception {
        System.out.println("=== Starting High Concurrency Stress Test ===");
        
        int totalTasks = 1000;
        int concurrentSubmissions = 100;
        
        Instant startTime = Instant.now();
        
        // Submit tasks in batches to simulate high concurrency
        List<CompletableFuture<Void>> submissionFutures = new ArrayList<>();
        
        for (int batch = 0; batch < totalTasks / concurrentSubmissions; batch++) {
            CompletableFuture<Void> batchFuture = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < concurrentSubmissions; i++) {
                    try {
                        Task task = createRandomTask();
                        CompletableFuture<TaskResult> future = engine.submit(task);
                        
                        future.whenComplete((result, throwable) -> {
                            completedTasks.incrementAndGet();
                            if (throwable != null) {
                                failedTasks.incrementAndGet();
                            } else if (!result.isSuccess()) {
                                failedTasks.incrementAndGet();
                            }
                        });
                        
                        // Add small delay to prevent overwhelming the system
                        Thread.sleep(1);
                    } catch (Exception e) {
                        failedTasks.incrementAndGet();
                    }
                }
            }, testExecutor);
            
            submissionFutures.add(batchFuture);
        }
        
        // Wait for all submissions to complete
        CompletableFuture.allOf(submissionFutures.toArray(new CompletableFuture[0]))
            .get(60, TimeUnit.SECONDS);
        
        System.out.println("All tasks submitted. Waiting for completion...");
        
        // Wait for all tasks to complete with timeout
        long timeoutMs = 120000; // 2 minutes
        long startWait = System.currentTimeMillis();
        
        while (completedTasks.get() < totalTasks && 
               (System.currentTimeMillis() - startWait) < timeoutMs) {
            Thread.sleep(100);
            
            // Print progress every 5 seconds
            if ((System.currentTimeMillis() - startWait) % 5000 < 100) {
                EngineStatistics stats = engine.getStatistics();
                System.out.printf("Progress: %d/%d completed, %d failed, Queue: %d, Running: %d%n",
                    completedTasks.get(), totalTasks, failedTasks.get(),
                    stats.getQueueSize(), stats.getTasksRunning());
            }
        }
        
        Instant endTime = Instant.now();
        Duration totalDuration = Duration.between(startTime, endTime);
        
        // Validate results
        System.out.println("=== Stress Test Results ===");
        System.out.printf("Total Tasks: %d%n", totalTasks);
        System.out.printf("Completed: %d%n", completedTasks.get());
        System.out.printf("Failed: %d%n", failedTasks.get());
        System.out.printf("Success Rate: %.2f%%%n", 
            (double)(completedTasks.get() - failedTasks.get()) / totalTasks * 100);
        System.out.printf("Total Duration: %d ms%n", totalDuration.toMillis());
        System.out.printf("Throughput: %.2f tasks/second%n", 
            (double)totalTasks / totalDuration.toMillis() * 1000);
        
        EngineStatistics finalStats = engine.getStatistics();
        System.out.printf("Final Queue Size: %d%n", finalStats.getQueueSize());
        System.out.printf("Final Running Tasks: %d%n", finalStats.getTasksRunning());
        System.out.printf("Total Retries: %d%n", retryCount.get());
        
        // Assertions
        assertTrue(completedTasks.get() >= totalTasks * 0.95, 
            "At least 95% of tasks should complete");
        assertTrue(failedTasks.get() <= totalTasks * 0.1, 
            "No more than 10% of tasks should fail");
        assertTrue(finalStats.getQueueSize() == 0, 
            "All tasks should be processed from queue");
        assertTrue(finalStats.getTasksRunning() == 0, 
            "No tasks should be running after completion");
    }

    @Test
    @Timeout(300)
    void testMixedWorkloadStress() throws Exception {
        System.out.println("=== Starting Mixed Workload Stress Test ===");
        
        int fastTasks = 500;
        int slowTasks = 200;
        int failingTasks = 100;
        int retryTasks = 200;
        int totalTasks = fastTasks + slowTasks + failingTasks + retryTasks;
        
        Instant startTime = Instant.now();
        
        // Submit different types of tasks concurrently
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // Fast tasks
        for (int i = 0; i < fastTasks; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Task task = TaskImpl.builder()
                        .type("fast-task")
                        .payload(createPayload("fast", taskIndex))
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    result.get(30, TimeUnit.SECONDS);
                    completedTasks.incrementAndGet();
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                }
            }, testExecutor);
            futures.add(future);
        }
        
        // Slow tasks
        for (int i = 0; i < slowTasks; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Task task = TaskImpl.builder()
                        .type("slow-task")
                        .payload(createPayload("slow", taskIndex))
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    result.get(60, TimeUnit.SECONDS);
                    completedTasks.incrementAndGet();
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                }
            }, testExecutor);
            futures.add(future);
        }
        
        // Failing tasks
        for (int i = 0; i < failingTasks; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Task task = TaskImpl.builder()
                        .type("failing-task")
                        .payload(createPayload("failing", taskIndex))
                        .maxRetries(2)
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    TaskResult taskResult = result.get(30, TimeUnit.SECONDS);
                    completedTasks.incrementAndGet();
                    if (!taskResult.isSuccess()) {
                        failedTasks.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                }
            }, testExecutor);
            futures.add(future);
        }
        
        // Retry tasks
        for (int i = 0; i < retryTasks; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Task task = TaskImpl.builder()
                        .type("retry-task")
                        .payload(createPayload("retry", taskIndex))
                        .maxRetries(3)
                        .retryDelayMs(50)
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    TaskResult taskResult = result.get(60, TimeUnit.SECONDS);
                    completedTasks.incrementAndGet();
                    if (!taskResult.isSuccess()) {
                        failedTasks.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                }
            }, testExecutor);
            futures.add(future);
        }
        
        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(180, TimeUnit.SECONDS);
        
        Instant endTime = Instant.now();
        Duration totalDuration = Duration.between(startTime, endTime);
        
        // Print results
        System.out.println("=== Mixed Workload Results ===");
        System.out.printf("Total Tasks: %d%n", totalTasks);
        System.out.printf("Completed: %d%n", completedTasks.get());
        System.out.printf("Failed: %d%n", failedTasks.get());
        System.out.printf("Duration: %d ms%n", totalDuration.toMillis());
        System.out.printf("Throughput: %.2f tasks/second%n", 
            (double)totalTasks / totalDuration.toMillis() * 1000);
        
        // Validate results
        assertTrue(completedTasks.get() >= totalTasks * 0.9, 
            "At least 90% of tasks should complete");
    }

    @Test
    @Timeout(300)
    void testSystemRestartRecovery() throws Exception {
        System.out.println("=== Starting System Restart Recovery Test ===");
        
        int tasksBeforeRestart = 100;
        int tasksAfterRestart = 50;
        
        // Submit tasks before restart
        List<UUID> taskIds = new ArrayList<>();
        for (int i = 0; i < tasksBeforeRestart; i++) {
            Task task = createRandomTask();
            taskIds.add(task.getId());
            engine.submit(task);
        }
        
        // Wait a bit for some tasks to be processed
        Thread.sleep(2000);
        
        // Stop the engine
        engine.stop();
        
        // Restart with same configuration
        TaskExecutionConfig config = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                10, 20, java.time.Duration.ofMinutes(1), 1000, java.time.Duration.ofSeconds(30)
            ))
            .queueConfig(new TaskExecutionConfig.QueueConfig(
                "/tmp/stress-test-" + UUID.randomUUID() + ".db", true, 1000, java.time.Duration.ofMinutes(5)
            ))
            .build();
        
        engine = TaskExecutionEngineFactory.create(config);
        engine.registerHandler(new FastTaskHandler());
        engine.registerHandler(new SlowTaskHandler());
        engine.registerHandler(new FailingTaskHandler());
        engine.registerHandler(new RetryTaskHandler());
        
        // Start the engine
        engine.start();
        
        // Submit more tasks after restart
        for (int i = 0; i < tasksAfterRestart; i++) {
            Task task = createRandomTask();
            taskIds.add(task.getId());
            engine.submit(task);
        }
        
        // Wait for all tasks to complete
        Thread.sleep(10000);
        
        // Check task statuses
        int completedCount = 0;
        int failedCount = 0;
        
        for (UUID taskId : taskIds) {
            try {
                CompletableFuture<TaskStatus> statusFuture = engine.getTaskStatus(taskId);
                TaskStatus status = statusFuture.get(5, TimeUnit.SECONDS);
                if (status == TaskStatus.COMPLETED) {
                    completedCount++;
                } else if (status == TaskStatus.FAILED || status == TaskStatus.CANCELLED) {
                    failedCount++;
                }
            } catch (Exception e) {
                failedCount++;
            }
        }
        
        System.out.printf("Tasks before restart: %d%n", tasksBeforeRestart);
        System.out.printf("Tasks after restart: %d%n", tasksAfterRestart);
        System.out.printf("Completed: %d%n", completedCount);
        System.out.printf("Failed: %d%n", failedCount);
        
        // Validate recovery
        assertTrue(completedCount >= (tasksBeforeRestart + tasksAfterRestart) * 0.8, 
            "At least 80% of tasks should complete after restart");
    }

    private Task createRandomTask() {
        String[] taskTypes = {"fast-task", "slow-task", "failing-task", "retry-task"};
        String taskType = taskTypes[new Random().nextInt(taskTypes.length)];
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("taskId", UUID.randomUUID().toString());
        payload.put("timestamp", System.currentTimeMillis());
        
        // For failing tasks, make them actually fail
        if ("failing-task".equals(taskType)) {
            payload.put("shouldFail", true);
        }
        
        return TaskImpl.builder()
            .type(taskType)
            .payload(payload)
            .maxRetries(2)
            .retryDelayMs(100)
            .build();
    }

    private Map<String, Object> createPayload(String type, int index) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", type);
        payload.put("index", index);
        payload.put("timestamp", System.currentTimeMillis());
        return payload;
    }

    // Test handlers
    private static class FastTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            return CompletableFuture.completedFuture(
                TaskResult.success(Map.of("result", "fast-completed"), 10L)
            );
        }

        @Override
        public String getSupportedTaskType() {
            return "fast-task";
        }
    }

    private static class SlowTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            try {
                Thread.sleep(100 + new Random().nextInt(200)); // 100-300ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return CompletableFuture.completedFuture(
                TaskResult.success(Map.of("result", "slow-completed"), 200L)
            );
        }

        @Override
        public String getSupportedTaskType() {
            return "slow-task";
        }
    }

    private static class FailingTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            return CompletableFuture.completedFuture(
                TaskResult.failure("Simulated failure", new RuntimeException("Test error"), true, 50L)
            );
        }

        @Override
        public String getSupportedTaskType() {
            return "failing-task";
        }
    }

    private static class RetryTaskHandler implements TaskHandler {
        private final AtomicInteger attemptCount = new AtomicInteger(0);

        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            int attempt = attemptCount.incrementAndGet();
            
            // Fail first 2 attempts, succeed on 3rd
            if (attempt <= 2) {
                return CompletableFuture.completedFuture(
                    TaskResult.failure("Retry attempt " + attempt, new RuntimeException("Retry error"), true, 30L)
                );
            } else {
                return CompletableFuture.completedFuture(
                    TaskResult.success(Map.of("result", "retry-success", "attempts", attempt), 50L)
                );
            }
        }

        @Override
        public String getSupportedTaskType() {
            return "retry-task";
        }
    }
}