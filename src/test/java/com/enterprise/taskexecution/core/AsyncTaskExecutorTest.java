package com.enterprise.taskexecution.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class AsyncTaskExecutorTest {
    
    private AsyncTaskExecutor executor;
    private BlockingQueue<Runnable> workQueue;
    
    @BeforeEach
    void setUp() {
        workQueue = new LinkedBlockingQueue<>(10);
        executor = new AsyncTaskExecutor(2, 4, 1000, TimeUnit.MILLISECONDS, workQueue);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown().get(5, TimeUnit.SECONDS);
        }
    }
    
    @Test
    void testTaskExecution() throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("test", "data");
        
        Task task = TaskImpl.builder()
            .type("test-task")
            .payload(payload)
            .timeoutMs(5000)
            .build();
        
        TaskHandler handler = new TestTaskHandler();
        
        CompletableFuture<TaskResult> future = executor.execute(task, handler);
        TaskResult result = future.get(10, TimeUnit.SECONDS);
        
        assertTrue(result.isSuccess());
        assertEquals("test-task", result.getResultData().get("taskType"));
        assertTrue(result.getExecutionDurationMs() > 0);
    }
    
    @Test
    void testTaskExecutionFailure() throws Exception {
        Task task = TaskImpl.builder()
            .type("failing-task")
            .timeoutMs(5000)
            .build();
        
        TaskHandler handler = new FailingTaskHandler();
        
        CompletableFuture<TaskResult> future = executor.execute(task, handler);
        TaskResult result = future.get(10, TimeUnit.SECONDS);
        
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.shouldRetry());
    }
    
    @Test
    void testTaskTimeout() throws Exception {
        Task task = TaskImpl.builder()
            .type("slow-task")
            .timeoutMs(100) // Very short timeout
            .build();
        
        TaskHandler handler = new SlowTaskHandler();
        
        CompletableFuture<TaskResult> future = executor.execute(task, handler);
        TaskResult result = future.get(5, TimeUnit.SECONDS);
        
        assertFalse(result.isSuccess());
        assertTrue(result.getErrorMessage().contains("timed out"));
        assertTrue(result.shouldRetry());
    }
    
    @Test
    void testExecutorStatistics() throws Exception {
        AsyncTaskExecutor.ExecutorStatistics stats = executor.getStatistics();
        
        assertTrue(stats.getTotalTasksExecuted() >= 0);
        assertTrue(stats.getTotalTasksCompleted() >= 0);
        assertTrue(stats.getTotalTasksFailed() >= 0);
        assertTrue(stats.getActiveThreadCount() >= 0);
        assertTrue(stats.getPoolSize() >= 0);
        assertTrue(stats.getCorePoolSize() > 0);
        assertTrue(stats.getMaximumPoolSize() > 0);
        assertTrue(stats.getQueueSize() >= 0);
    }
    
    @Test
    void testIsRunning() {
        assertTrue(executor.isRunning());
    }
    
    // Test handlers
    private static class TestTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("taskType", task.getType());
            resultData.put("taskId", task.getId().toString());
            
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
            return CompletableFuture.completedFuture(
                TaskResult.failure("Simulated failure", new RuntimeException("Test error"), true, 50L)
            );
        }
        
        @Override
        public String getSupportedTaskType() {
            return "failing-task";
        }
    }
    
    private static class SlowTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            try {
                Thread.sleep(1000); // Sleep for 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("completed", true);
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, 1000L)
            );
        }
        
        @Override
        public String getSupportedTaskType() {
            return "slow-task";
        }
    }
}