package com.enterprise.taskexecution.dlq;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Dead Letter Queue functionality
 */
class DeadLetterQueueTest {
    
    @TempDir
    File tempDir;
    
    private MapDBDeadLetterQueue dlq;
    private Task testTask;
    
    @BeforeEach
    void setUp() {
        String dbPath = new File(tempDir, "test-dlq.db").getAbsolutePath();
        dlq = new MapDBDeadLetterQueue(dbPath, 1000, true, 30);
        
        testTask = TaskImpl.builder()
            .type("test-task")
            .payload(Map.of("key", "value"))
            .maxRetries(3)
            .retryCount(2)
            .build();
    }
    
    @Test
    void testAddToDeadLetterQueue() {
        // Add task to DLQ
        boolean result = dlq.addToDeadLetterQueue(testTask, "Test failure", Instant.now());
        
        assertTrue(result);
        assertEquals(1, dlq.getDeadLetterQueueSize());
    }
    
    @Test
    void testGetDeadLetterEntry() {
        // Add task to DLQ
        dlq.addToDeadLetterQueue(testTask, "Test failure", Instant.now());
        
        // Retrieve entry
        Optional<DeadLetterEntry> entry = dlq.getDeadLetterEntry(testTask.getId());
        
        assertTrue(entry.isPresent());
        assertEquals(testTask.getId(), entry.get().getTaskId());
        assertEquals("Test failure", entry.get().getFailureReason());
        assertEquals(testTask.getId(), entry.get().getOriginalTask().getId());
        assertEquals(testTask.getType(), entry.get().getOriginalTask().getType());
        assertEquals(2, entry.get().getRetryCount());
    }
    
    @Test
    void testGetAllDeadLetterEntries() {
        // Add multiple tasks
        Task task1 = TaskImpl.builder().type("task1").payload(Map.of()).build();
        Task task2 = TaskImpl.builder().type("task2").payload(Map.of()).build();
        
        dlq.addToDeadLetterQueue(task1, "Failure 1", Instant.now());
        dlq.addToDeadLetterQueue(task2, "Failure 2", Instant.now());
        
        List<DeadLetterEntry> entries = dlq.getAllDeadLetterEntries();
        
        assertEquals(2, entries.size());
        assertTrue(entries.stream().anyMatch(e -> e.getTaskId().equals(task1.getId())));
        assertTrue(entries.stream().anyMatch(e -> e.getTaskId().equals(task2.getId())));
    }
    
    @Test
    void testGetDeadLetterEntriesWithPagination() {
        // Add multiple tasks
        for (int i = 0; i < 5; i++) {
            Task task = TaskImpl.builder()
                .type("task" + i)
                .payload(Map.of("index", i))
                .build();
            dlq.addToDeadLetterQueue(task, "Failure " + i, Instant.now());
        }
        
        // Test pagination
        List<DeadLetterEntry> page1 = dlq.getDeadLetterEntries(0, 2);
        List<DeadLetterEntry> page2 = dlq.getDeadLetterEntries(2, 2);
        
        assertEquals(2, page1.size());
        assertEquals(2, page2.size());
        assertEquals(5, dlq.getDeadLetterQueueSize());
    }
    
    @Test
    void testRemoveFromDeadLetterQueue() {
        // Add task to DLQ
        dlq.addToDeadLetterQueue(testTask, "Test failure", Instant.now());
        assertEquals(1, dlq.getDeadLetterQueueSize());
        
        // Remove task
        boolean result = dlq.removeFromDeadLetterQueue(testTask.getId());
        
        assertTrue(result);
        assertEquals(0, dlq.getDeadLetterQueueSize());
        
        // Try to remove non-existent task
        boolean result2 = dlq.removeFromDeadLetterQueue(UUID.randomUUID());
        assertFalse(result2);
    }
    
    @Test
    void testClearDeadLetterQueue() {
        // Add multiple tasks
        for (int i = 0; i < 3; i++) {
            Task task = TaskImpl.builder()
                .type("task" + i)
                .payload(Map.of())
                .build();
            dlq.addToDeadLetterQueue(task, "Failure " + i, Instant.now());
        }
        
        assertEquals(3, dlq.getDeadLetterQueueSize());
        
        // Clear DLQ
        int cleared = dlq.clearDeadLetterQueue();
        
        assertEquals(3, cleared);
        assertEquals(0, dlq.getDeadLetterQueueSize());
    }
    
    @Test
    void testIsAtCapacity() {
        // Create DLQ with capacity of 2
        String dbPath = new File(tempDir, "capacity-test.db").getAbsolutePath();
        MapDBDeadLetterQueue smallDlq = new MapDBDeadLetterQueue(dbPath, 2, true, 30);
        
        // Add tasks up to capacity
        Task task1 = TaskImpl.builder().type("task1").payload(Map.of()).build();
        Task task2 = TaskImpl.builder().type("task2").payload(Map.of()).build();
        
        assertFalse(smallDlq.isAtCapacity());
        
        smallDlq.addToDeadLetterQueue(task1, "Failure 1", Instant.now());
        assertFalse(smallDlq.isAtCapacity());
        
        smallDlq.addToDeadLetterQueue(task2, "Failure 2", Instant.now());
        assertTrue(smallDlq.isAtCapacity());
        
        // Try to add beyond capacity
        Task task3 = TaskImpl.builder().type("task3").payload(Map.of()).build();
        boolean result = smallDlq.addToDeadLetterQueue(task3, "Failure 3", Instant.now());
        assertFalse(result);
    }
    
    @Test
    void testGetStatistics() {
        // Create a fresh DLQ for this test
        String dbPath = new File(tempDir, "stats-test.db").getAbsolutePath();
        MapDBDeadLetterQueue statsDlq = new MapDBDeadLetterQueue(dbPath, 1000, true, 30);
        
        // Add tasks with different types and error types
        Task task1 = TaskImpl.builder().type("email-task").payload(Map.of()).build();
        Task task2 = TaskImpl.builder().type("email-task").payload(Map.of()).build();
        Task task3 = TaskImpl.builder().type("report-task").payload(Map.of()).build();
        
        statsDlq.addToDeadLetterQueue(task1, "SMTP Error", Instant.now());
        statsDlq.addToDeadLetterQueue(task2, "SMTP Error", Instant.now());
        statsDlq.addToDeadLetterQueue(task3, "Database Error", Instant.now());
        
        DeadLetterQueueStatistics stats = statsDlq.getStatistics();
        
        assertEquals(3, stats.getCurrentSize());
        assertEquals(3, stats.getTotalAdded());
        assertEquals(0, stats.getTotalRemoved());
        
        Map<String, Integer> errorTypeCounts = stats.getErrorTypeCounts();
        assertEquals(3, errorTypeCounts.get("Unknown"));
        
        Map<String, Integer> taskTypeCounts = stats.getTaskTypeCounts();
        assertEquals(2, taskTypeCounts.get("email-task"));
        assertEquals(1, taskTypeCounts.get("report-task"));
    }
    
    @Test
    void testCleanupOldEntries() throws InterruptedException {
        // Create DLQ with 1-day retention
        String dbPath = new File(tempDir, "cleanup-test.db").getAbsolutePath();
        MapDBDeadLetterQueue cleanupDlq = new MapDBDeadLetterQueue(dbPath, 1000, true, 1);
        
        // Add task normally first
        Task oldTask = TaskImpl.builder().type("old-task").payload(Map.of()).build();
        cleanupDlq.addToDeadLetterQueue(oldTask, "Old failure", Instant.now());
        
        assertEquals(1, cleanupDlq.getDeadLetterQueueSize());
        
        // Wait for 2 seconds to ensure the entry is old enough for 1-day retention
        Thread.sleep(2000);
        
        // Run cleanup
        cleanupDlq.cleanupOldEntries();
        
        // The entry should still be there since it's only 2 seconds old, not 1 day
        assertEquals(1, cleanupDlq.getDeadLetterQueueSize());
        
        // Now test with a very short retention period
        MapDBDeadLetterQueue shortRetentionDlq = new MapDBDeadLetterQueue(
            new File(tempDir, "short-retention-test.db").getAbsolutePath(), 
            1000, true, 0); // 0 days retention
        
        Task newTask = TaskImpl.builder().type("new-task").payload(Map.of()).build();
        shortRetentionDlq.addToDeadLetterQueue(newTask, "New failure", Instant.now());
        
        assertEquals(1, shortRetentionDlq.getDeadLetterQueueSize());
        
        // Run cleanup - should remove the entry
        shortRetentionDlq.cleanupOldEntries();
        
        assertEquals(0, shortRetentionDlq.getDeadLetterQueueSize());
    }
    
    @Test
    void testDeadLetterEntryCreation() {
        RuntimeException error = new RuntimeException("Test error");
        DeadLetterEntry entry = DeadLetterEntry.create(testTask, "Test failure", Instant.now(), 3, error);
        
        assertEquals(testTask.getId(), entry.getTaskId());
        assertEquals(testTask, entry.getOriginalTask());
        assertEquals("Test failure", entry.getFailureReason());
        assertEquals(3, entry.getRetryCount());
        assertEquals("RuntimeException", entry.getErrorType());
        assertNotNull(entry.getStackTrace());
        assertTrue(entry.getStackTrace().contains("Test error"));
    }
    
    @Test
    void testDeadLetterEntryEquality() {
        DeadLetterEntry entry1 = DeadLetterEntry.create(testTask, "Failure 1", Instant.now(), 1, null);
        DeadLetterEntry entry2 = DeadLetterEntry.create(testTask, "Failure 2", Instant.now(), 2, null);
        
        assertEquals(entry1, entry2); // Same task ID
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }
    
    @Test
    void testClose() {
        dlq.addToDeadLetterQueue(testTask, "Test failure", Instant.now());
        assertEquals(1, dlq.getDeadLetterQueueSize());
        
        // Close should not throw exception
        assertDoesNotThrow(() -> dlq.close());
    }
}