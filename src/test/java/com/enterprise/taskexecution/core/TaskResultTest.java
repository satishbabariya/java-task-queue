package com.enterprise.taskexecution.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

class TaskResultTest {
    
    @Test
    void testSuccessResult() {
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("output", "success");
        resultData.put("count", 100);
        
        TaskResult result = TaskResult.success(resultData, 1500L);
        
        assertTrue(result.isSuccess());
        assertNull(result.getErrorMessage());
        assertNull(result.getException());
        assertEquals(resultData, result.getResultData());
        assertNotNull(result.getCompletedAt());
        assertEquals(1500L, result.getExecutionDurationMs());
        assertFalse(result.shouldRetry());
    }
    
    @Test
    void testFailureResult() {
        RuntimeException exception = new RuntimeException("Test error");
        TaskResult result = TaskResult.failure("Task failed", exception, true, 2000L);
        
        assertFalse(result.isSuccess());
        assertEquals("Task failed", result.getErrorMessage());
        assertEquals(exception, result.getException());
        assertNull(result.getResultData());
        assertNotNull(result.getCompletedAt());
        assertEquals(2000L, result.getExecutionDurationMs());
        assertTrue(result.shouldRetry());
    }
    
    @Test
    void testFailureResultNoRetry() {
        TaskResult result = TaskResult.failure("Permanent failure", null, false, 1000L);
        
        assertFalse(result.isSuccess());
        assertEquals("Permanent failure", result.getErrorMessage());
        assertNull(result.getException());
        assertFalse(result.shouldRetry());
    }
}