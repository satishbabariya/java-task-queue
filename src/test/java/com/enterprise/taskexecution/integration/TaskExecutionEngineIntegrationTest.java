package com.enterprise.taskexecution.integration;

import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.core.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class TaskExecutionEngineIntegrationTest {
    
    private TaskExecutionEngine engine;
    
    @BeforeEach
    void setUp() {
        engine = TaskExecutionEngineFactory.createDefault();
        engine.registerHandler(new TestTaskHandler());
        engine.start();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.stop().get(10, TimeUnit.SECONDS);
        }
    }
    
    @Test
    void testTaskSubmissionAndExecution() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("message", "Hello World");
        payload.put("timestamp", Instant.now().toString());
        
        Task task = TaskImpl.builder()
            .type("test-task")
            .payload(payload)
            .priority(5)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        TaskResult result = future.get(10, TimeUnit.SECONDS);
        
        assertTrue(result.isSuccess());
        assertEquals("test-task", result.getResultData().get("taskType"));
        assertEquals(task.getId().toString(), result.getResultData().get("taskId"));
    }
    
    @Test
    void testTaskScheduling() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("scheduled", true);
        
        Task task = TaskImpl.builder()
            .type("test-task")
            .payload(payload)
            .scheduledFor(Instant.now().plusSeconds(1))
            .build();
        
        CompletableFuture<Void> future = engine.schedule(task);
        future.get(10, TimeUnit.SECONDS);
        
        // Wait a bit for the task to execute
        Thread.sleep(2000);
        
        TaskStatus status = engine.getTaskStatus(task.getId()).get(5, TimeUnit.SECONDS);
        assertTrue(status == TaskStatus.COMPLETED || status == TaskStatus.RUNNING);
    }
    
    @Test
    void testTaskCancellation() throws Exception {
        Task task = TaskImpl.builder()
            .type("test-task")
            .payload(new HashMap<>())
            .scheduledFor(Instant.now().plusSeconds(10))
            .build();
        
        CompletableFuture<Void> future = engine.schedule(task);
        
        // Cancel the task immediately
        CompletableFuture<Boolean> cancelled = engine.cancel(task.getId());
        assertTrue(cancelled.get(5, TimeUnit.SECONDS));
        
        TaskStatus status = engine.getTaskStatus(task.getId()).get(5, TimeUnit.SECONDS);
        assertEquals(TaskStatus.CANCELLED, status);
    }
    
    @Test
    void testTaskRetry() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        
        Task task = TaskImpl.builder()
            .type("failing-task")
            .payload(payload)
            .maxRetries(2)
            .retryDelayMs(100)
            .build();
        
        engine.registerHandler(new FailingTaskHandler());
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        TaskResult result = future.get(15, TimeUnit.SECONDS);
        
        // The task should eventually fail after retries
        assertFalse(result.isSuccess());
    }
    
    @Test
    void testEngineStatistics() throws Exception {
        // Submit a few tasks
        for (int i = 0; i < 3; i++) {
            Task task = TaskImpl.builder()
                .type("test-task")
                .payload(new HashMap<>())
                .build();
            
            engine.submit(task);
        }
        
        // Wait for tasks to complete
        Thread.sleep(2000);
        
        EngineStatistics stats = engine.getStatistics();
        
        assertTrue(stats.getTotalTasksSubmitted() >= 3);
        assertTrue(stats.getTotalTasksCompleted() >= 0);
        assertTrue(stats.getTasksPending() >= 0);
        assertTrue(stats.getTasksRunning() >= 0);
        assertTrue(stats.getUptimeMs() > 0);
        assertNotNull(stats.getStartedAt());
    }
    
    @Test
    void testMultipleTaskTypes() throws Exception {
        // Register multiple handlers
        engine.registerHandler(new TestTaskHandler());
        engine.registerHandler(new FailingTaskHandler());
        
        // Submit different task types
        Task task1 = TaskImpl.builder().type("test-task").payload(new HashMap<>()).build();
        
        // Make the failing task actually fail
        Map<String, Object> failingPayload = new HashMap<>();
        failingPayload.put("shouldFail", true);
        Task task2 = TaskImpl.builder().type("failing-task").payload(failingPayload).build();
        
        CompletableFuture<TaskResult> future1 = engine.submit(task1);
        CompletableFuture<TaskResult> future2 = engine.submit(task2);
        
        TaskResult result1 = future1.get(10, TimeUnit.SECONDS);
        TaskResult result2 = future2.get(10, TimeUnit.SECONDS);
        
        assertTrue(result1.isSuccess());
        assertFalse(result2.isSuccess());
    }
    
    // Test handlers
    private static class TestTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("taskType", task.getType());
            resultData.put("taskId", task.getId().toString());
            resultData.put("processed", true);
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, 100L)
            );
        }
        
        @Override
        public String getSupportedTaskType() {
            return "test-task";
        }
    }
    
    private static class FailingTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            Map<String, Object> payload = task.getPayload();
            boolean shouldFail = Boolean.TRUE.equals(payload.get("shouldFail"));
            
            if (shouldFail) {
                return CompletableFuture.completedFuture(
                    TaskResult.failure("Simulated failure", new RuntimeException("Test error"), true, 50L)
                );
            }
            
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("taskType", task.getType());
            resultData.put("taskId", task.getId().toString());
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, 100L)
            );
        }
        
        @Override
        public String getSupportedTaskType() {
            return "failing-task";
        }
    }
}