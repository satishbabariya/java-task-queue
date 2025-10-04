package com.enterprise.taskexecution.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class TaskImplTest {
    
    private Map<String, Object> testPayload;
    private Map<String, Object> testMetadata;
    
    @BeforeEach
    void setUp() {
        testPayload = new HashMap<>();
        testPayload.put("message", "Hello World");
        testPayload.put("count", 42);
        
        testMetadata = new HashMap<>();
        testMetadata.put("source", "test");
        testMetadata.put("priority", "high");
    }
    
    @Test
    void testTaskCreation() {
        Task task = TaskImpl.builder()
            .type("test-task")
            .payload(testPayload)
            .priority(10)
            .maxRetries(3)
            .timeoutMs(30000)
            .metadata(testMetadata)
            .build();
        
        assertNotNull(task.getId());
        assertEquals("test-task", task.getType());
        assertEquals(testPayload, task.getPayload());
        assertEquals(10, task.getPriority());
        assertEquals(3, task.getMaxRetries());
        assertEquals(0, task.getRetryCount());
        assertEquals(30000, task.getTimeoutMs());
        assertEquals(testMetadata, task.getMetadata());
        assertNotNull(task.getCreatedAt());
    }
    
    @Test
    void testTaskWithRetryCount() {
        Task originalTask = TaskImpl.builder()
            .type("test-task")
            .payload(testPayload)
            .build();
        
        Task retryTask = originalTask.withRetryCount(2);
        
        assertEquals(originalTask.getId(), retryTask.getId());
        assertEquals(originalTask.getType(), retryTask.getType());
        assertEquals(originalTask.getPayload(), retryTask.getPayload());
        assertEquals(2, retryTask.getRetryCount());
    }
    
    @Test
    void testTaskWithScheduledFor() {
        Instant scheduledTime = Instant.now().plusSeconds(3600);
        
        Task originalTask = TaskImpl.builder()
            .type("test-task")
            .payload(testPayload)
            .build();
        
        Task scheduledTask = originalTask.withScheduledFor(scheduledTime);
        
        assertEquals(originalTask.getId(), scheduledTask.getId());
        assertEquals(scheduledTime, scheduledTask.getScheduledFor());
    }
    
    @Test
    void testTaskBuilderValidation() {
        // Test missing type
        assertThrows(IllegalArgumentException.class, () -> {
            TaskImpl.builder()
                .payload(testPayload)
                .build();
        });
    }
    
    @Test
    void testTaskDefaults() {
        Task task = TaskImpl.builder()
            .type("test-task")
            .build();
        
        assertEquals(0, task.getPriority());
        assertEquals(3, task.getMaxRetries());
        assertEquals(0, task.getRetryCount());
        assertEquals(5000, task.getRetryDelayMs());
        assertEquals(30000, task.getTimeoutMs());
        assertNull(task.getScheduledFor());
    }
}