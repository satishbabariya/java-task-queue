package com.enterprise.taskexecution.integration;

import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.config.TaskExecutionConfig;
import com.enterprise.taskexecution.core.*;
import com.enterprise.taskexecution.dlq.DeadLetterEntry;
import com.enterprise.taskexecution.dlq.DeadLetterQueueStatistics;
import com.enterprise.taskexecution.exception.HandlerNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Dead Letter Queue functionality
 */
class DeadLetterQueueIntegrationTest {
    
    @TempDir
    File tempDir;
    
    private TaskExecutionEngine engine;
    
    @BeforeEach
    void setUp() {
        // Configure engine with DLQ enabled
        TaskExecutionConfig config = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                4, 8, Duration.ofMinutes(1), 100, Duration.ofSeconds(30)
            ))
            .queueConfig(new TaskExecutionConfig.QueueConfig(
                new File(tempDir, "test-queue.db").getAbsolutePath(),
                true, 1000, Duration.ofMinutes(5)
            ))
            .retryConfig(new TaskExecutionConfig.RetryConfig(
                2, Duration.ofMillis(100), 2.0, Duration.ofMillis(1000), true
            ))
            .dlqConfig(new TaskExecutionConfig.DeadLetterQueueConfig(
                true,
                new File(tempDir, "test-dlq.db").getAbsolutePath(),
                1000,
                true,
                30,
                Duration.ofHours(6)
            ))
            .build();
        
        engine = TaskExecutionEngineFactory.create(config);
        engine.start();
    }
    
    @Test
    void testTaskMovedToDlqOnHandlerNotFound() throws Exception {
        // Submit task without registering handler
        Task task = TaskImpl.builder()
            .type("unknown-task")
            .payload(new HashMap<>())
            .maxRetries(1)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        
        // Wait for task to fail
        try {
            TaskResult result = future.get(5, TimeUnit.SECONDS);
            assertFalse(result.isSuccess());
        } catch (ExecutionException e) {
            // Expected - task should fail with HandlerNotFoundException
            assertTrue(e.getCause() instanceof HandlerNotFoundException);
        }
        
        // Check if task is in DLQ
        Optional<DeadLetterEntry> dlqEntry = engine.getDeadLetterEntry(task.getId()).get();
        assertTrue(dlqEntry.isPresent());
        assertEquals(task.getId(), dlqEntry.get().getTaskId());
        assertTrue(dlqEntry.get().getFailureReason().contains("No handler found"));
    }
    
    @Test
    void testTaskMovedToDlqAfterRetriesExhausted() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        // Submit task that will fail
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        Task task = TaskImpl.builder()
            .type("failing-task")
            .payload(payload)
            .maxRetries(2)
            .retryDelayMs(50)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        
        // Wait for task to fail after retries
        try {
            TaskResult result = future.get(10, TimeUnit.SECONDS);
            assertFalse(result.isSuccess());
        } catch (ExecutionException e) {
            // Expected - task should fail after retries
            assertTrue(e.getCause() instanceof RuntimeException);
        }
        
        // Wait a bit for DLQ operation to complete
        Thread.sleep(500);
        
        // Check DLQ statistics first
        DeadLetterQueueStatistics stats = engine.getDeadLetterQueueStatistics().get();
        assertTrue(stats.getCurrentSize() > 0, "DLQ should have at least one entry");
        
        // Check if task is in DLQ
        Optional<DeadLetterEntry> dlqEntry = engine.getDeadLetterEntry(task.getId()).get();
        assertTrue(dlqEntry.isPresent(), "Task should be in DLQ");
        assertEquals(task.getId(), dlqEntry.get().getTaskId());
        
        assertTrue(dlqEntry.get().getFailureReason().contains("Retry failed"));
    }
    
    @Test
    void testTaskMovedToDlqOnExecutionException() throws Exception {
        // Register handler that throws exception
        engine.registerHandler(new ExceptionThrowingHandler());
        
        Task task = TaskImpl.builder()
            .type("exception-task")
            .payload(new HashMap<>())
            .maxRetries(1)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        
        // Wait for task to fail
        TaskResult result = future.get(5, TimeUnit.SECONDS);
        assertFalse(result.isSuccess());
        
        // Check if task is in DLQ
        Optional<DeadLetterEntry> dlqEntry = engine.getDeadLetterEntry(task.getId()).get();
        assertTrue(dlqEntry.isPresent());
        assertTrue(dlqEntry.get().getFailureReason().contains("Task execution failed"));
    }
    
    @Test
    void testGetAllDeadLetterEntries() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        // Submit multiple failing tasks
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        
        Task task1 = TaskImpl.builder().type("failing-task").payload(payload).maxRetries(1).build();
        Task task2 = TaskImpl.builder().type("failing-task").payload(payload).maxRetries(1).build();
        Task task3 = TaskImpl.builder().type("failing-task").payload(payload).maxRetries(1).build();
        
        CompletableFuture<TaskResult> future1 = engine.submit(task1);
        CompletableFuture<TaskResult> future2 = engine.submit(task2);
        CompletableFuture<TaskResult> future3 = engine.submit(task3);
        
        // Wait for all tasks to fail
        future1.get(5, TimeUnit.SECONDS);
        future2.get(5, TimeUnit.SECONDS);
        future3.get(5, TimeUnit.SECONDS);
        
        // Get all DLQ entries
        List<DeadLetterEntry> entries = engine.getAllDeadLetterEntries().get();
        assertEquals(3, entries.size());
        
        // Verify all tasks are in DLQ
        assertTrue(entries.stream().anyMatch(e -> e.getTaskId().equals(task1.getId())));
        assertTrue(entries.stream().anyMatch(e -> e.getTaskId().equals(task2.getId())));
        assertTrue(entries.stream().anyMatch(e -> e.getTaskId().equals(task3.getId())));
    }
    
    @Test
    void testGetDeadLetterEntriesWithPagination() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        // Submit multiple failing tasks
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        
        for (int i = 0; i < 5; i++) {
            Task task = TaskImpl.builder()
                .type("failing-task")
                .payload(payload)
                .maxRetries(1)
                .build();
            engine.submit(task).get(5, TimeUnit.SECONDS);
        }
        
        // Test pagination
        List<DeadLetterEntry> page1 = engine.getDeadLetterEntries(0, 2).get();
        List<DeadLetterEntry> page2 = engine.getDeadLetterEntries(2, 2).get();
        List<DeadLetterEntry> page3 = engine.getDeadLetterEntries(4, 2).get();
        
        assertEquals(2, page1.size());
        assertEquals(2, page2.size());
        assertEquals(1, page3.size());
    }
    
    @Test
    void testRemoveFromDeadLetterQueue() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        Task task = TaskImpl.builder()
            .type("failing-task")
            .payload(payload)
            .maxRetries(1)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        future.get(5, TimeUnit.SECONDS);
        
        // Verify task is in DLQ
        Optional<DeadLetterEntry> dlqEntry = engine.getDeadLetterEntry(task.getId()).get();
        assertTrue(dlqEntry.isPresent());
        
        // Remove from DLQ
        boolean removed = engine.removeFromDeadLetterQueue(task.getId()).get();
        assertTrue(removed);
        
        // Verify task is no longer in DLQ
        Optional<DeadLetterEntry> dlqEntryAfter = engine.getDeadLetterEntry(task.getId()).get();
        assertFalse(dlqEntryAfter.isPresent());
    }
    
    @Test
    void testClearDeadLetterQueue() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        
        // Submit multiple failing tasks
        for (int i = 0; i < 3; i++) {
            Task task = TaskImpl.builder()
                .type("failing-task")
                .payload(payload)
                .maxRetries(1)
                .build();
            engine.submit(task).get(5, TimeUnit.SECONDS);
        }
        
        // Verify tasks are in DLQ
        List<DeadLetterEntry> entries = engine.getAllDeadLetterEntries().get();
        assertEquals(3, entries.size());
        
        // Clear DLQ
        int cleared = engine.clearDeadLetterQueue().get();
        assertEquals(3, cleared);
        
        // Verify DLQ is empty
        List<DeadLetterEntry> entriesAfter = engine.getAllDeadLetterEntries().get();
        assertEquals(0, entriesAfter.size());
    }
    
    @Test
    void testGetDeadLetterQueueStatistics() throws Exception {
        // Register failing handler
        engine.registerHandler(new FailingTaskHandler());
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        
        // Submit tasks with different types
        Task task1 = TaskImpl.builder().type("email-task").payload(payload).maxRetries(1).build();
        Task task2 = TaskImpl.builder().type("email-task").payload(payload).maxRetries(1).build();
        Task task3 = TaskImpl.builder().type("report-task").payload(payload).maxRetries(1).build();
        
        engine.submit(task1).get(5, TimeUnit.SECONDS);
        engine.submit(task2).get(5, TimeUnit.SECONDS);
        engine.submit(task3).get(5, TimeUnit.SECONDS);
        
        // Get statistics
        DeadLetterQueueStatistics stats = engine.getDeadLetterQueueStatistics().get();
        
        assertEquals(3, stats.getCurrentSize());
        assertEquals(3, stats.getTotalAdded());
        assertEquals(0, stats.getTotalRemoved());
        
        Map<String, Integer> taskTypeCounts = stats.getTaskTypeCounts();
        assertEquals(2, taskTypeCounts.get("email-task"));
        assertEquals(1, taskTypeCounts.get("report-task"));
    }
    
    @Test
    void testRetryFromDeadLetterQueue() throws Exception {
        // Register failing handler first
        engine.registerHandler(new FailingTaskHandler());
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("shouldFail", true);
        Task task = TaskImpl.builder()
            .type("failing-task")
            .payload(payload)
            .maxRetries(1)
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        future.get(5, TimeUnit.SECONDS);
        
        // Verify task is in DLQ
        Optional<DeadLetterEntry> dlqEntry = engine.getDeadLetterEntry(task.getId()).get();
        assertTrue(dlqEntry.isPresent());
        
        // Register successful handler
        engine.registerHandler(new TestTaskHandler());
        
        // Retry from DLQ
        CompletableFuture<TaskResult> retryFuture = engine.retryFromDeadLetterQueue(task.getId());
        TaskResult retryResult = retryFuture.get(5, TimeUnit.SECONDS);
        
        assertTrue(retryResult.isSuccess());
        
        // Verify task is no longer in DLQ
        Optional<DeadLetterEntry> dlqEntryAfter = engine.getDeadLetterEntry(task.getId()).get();
        assertFalse(dlqEntryAfter.isPresent());
    }
    
    @Test
    void testRetryFromDeadLetterQueueWithNonExistentTask() {
        UUID nonExistentId = UUID.randomUUID();
        
        CompletableFuture<TaskResult> future = engine.retryFromDeadLetterQueue(nonExistentId);
        
        assertThrows(Exception.class, () -> {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (e.getCause() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getCause();
                }
                throw e;
            }
        });
    }
    
    // Test handlers
    
    private static class FailingTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            Map<String, Object> payload = task.getPayload();
            boolean shouldFail = (Boolean) payload.getOrDefault("shouldFail", false);
            
            if (shouldFail) {
                return CompletableFuture.completedFuture(
                    TaskResult.failure("Task failed as requested", null, true, 100)
                );
            }
            
            return CompletableFuture.completedFuture(TaskResult.success(Map.of("message", "Task completed"), 100));
        }
        
        @Override
        public String getSupportedTaskType() {
            return "failing-task";
        }
    }
    
    private static class ExceptionThrowingHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            throw new RuntimeException("Handler threw exception");
        }
        
        @Override
        public String getSupportedTaskType() {
            return "exception-task";
        }
    }
    
    private static class TestTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            return CompletableFuture.completedFuture(TaskResult.success(Map.of("message", "Task completed successfully"), 100));
        }
        
        @Override
        public String getSupportedTaskType() {
            return "failing-task";
        }
    }
}