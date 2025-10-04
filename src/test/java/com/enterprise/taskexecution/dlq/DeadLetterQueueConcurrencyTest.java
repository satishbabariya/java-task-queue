package com.enterprise.taskexecution.dlq;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskImpl;
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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive concurrency stress tests for DeadLetterQueue implementation.
 * Tests thread safety, performance, and data integrity under high concurrency.
 */
public class DeadLetterQueueConcurrencyTest {

    private DeadLetterQueue dlq;
    private ExecutorService executorService;
    private final int THREAD_COUNT = 50;
    private final int OPERATIONS_PER_THREAD = 1000;
    private final Duration TEST_DURATION = Duration.ofMinutes(2);

    @BeforeEach
    void setUp() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String uniqueName = UUID.randomUUID().toString();
        String path = tmpDir.endsWith("/") ? (tmpDir + "dlq-concurrency-" + uniqueName + ".db")
                                          : (tmpDir + "/dlq-concurrency-" + uniqueName + ".db");
        
        dlq = new MapDBDeadLetterQueue(path, 10000, true, 30);
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterEach
    void tearDown() {
        if (dlq != null) {
            dlq.close();
        }
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentAddOperations() throws InterruptedException {
        System.out.println("Testing concurrent add operations with " + THREAD_COUNT + " threads...");
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        Task task = createTestTask(threadId, j);
                        boolean success = dlq.addToDeadLetterQueue(task, "Concurrent test failure", Instant.now());
                        if (success) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    failureCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                  "Test timed out after " + TEST_DURATION);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("Concurrent add operations completed:");
        System.out.println("  Successes: " + successCount.get());
        System.out.println("  Failures: " + failureCount.get());
        System.out.println("  Duration: " + duration + "ms");
        System.out.println("  Throughput: " + (successCount.get() * 1000.0 / duration) + " ops/sec");
        
        // Verify data integrity
        DeadLetterQueueStatistics stats = dlq.getStatistics();
        assertEquals(successCount.get(), stats.getCurrentSize(), "DLQ size mismatch");
        assertEquals(successCount.get(), stats.getTotalAdded(), "Total added mismatch");
        
        assertTrue(successCount.get() > 0, "No successful operations");
        assertTrue(duration < TEST_DURATION.toMillis(), "Test took too long");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentMixedOperations() throws InterruptedException {
        System.out.println("Testing concurrent mixed operations with " + THREAD_COUNT + " threads...");
        
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger getCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        // Pre-populate DLQ with some entries
        Set<UUID> knownTaskIds = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 100; i++) {
            Task task = createTestTask(0, i);
            if (dlq.addToDeadLetterQueue(task, "Pre-populated", Instant.now())) {
                knownTaskIds.add(task.getId());
            }
        }
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        int operation = random.nextInt(3);
                        
                        switch (operation) {
                            case 0: // Add operation
                                Task task = createTestTask(threadId, j);
                                if (dlq.addToDeadLetterQueue(task, "Mixed test failure", Instant.now())) {
                                    addCount.incrementAndGet();
                                    knownTaskIds.add(task.getId());
                                }
                                break;
                            case 1: // Get operation
                                if (!knownTaskIds.isEmpty()) {
                                    UUID randomId = knownTaskIds.stream()
                                        .skip(random.nextInt(knownTaskIds.size()))
                                        .findFirst().orElse(null);
                                    if (randomId != null) {
                                        dlq.getDeadLetterEntry(randomId);
                                        getCount.incrementAndGet();
                                    }
                                }
                                break;
                            case 2: // Remove operation
                                if (!knownTaskIds.isEmpty()) {
                                    UUID randomId = knownTaskIds.stream()
                                        .skip(random.nextInt(knownTaskIds.size()))
                                        .findFirst().orElse(null);
                                    if (randomId != null && dlq.removeFromDeadLetterQueue(randomId)) {
                                        removeCount.incrementAndGet();
                                        knownTaskIds.remove(randomId);
                                    }
                                }
                                break;
                        }
                        
                        // Small random delay to increase chance of race conditions
                        if (random.nextInt(100) < 5) {
                            Thread.sleep(random.nextInt(10));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                  "Test timed out after " + TEST_DURATION);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("Concurrent mixed operations completed:");
        System.out.println("  Adds: " + addCount.get());
        System.out.println("  Gets: " + getCount.get());
        System.out.println("  Removes: " + removeCount.get());
        System.out.println("  Errors: " + errorCount.get());
        System.out.println("  Duration: " + duration + "ms");
        System.out.println("  Total operations: " + (addCount.get() + getCount.get() + removeCount.get()));
        System.out.println("  Throughput: " + ((addCount.get() + getCount.get() + removeCount.get()) * 1000.0 / duration) + " ops/sec");
        
        // Verify data integrity
        DeadLetterQueueStatistics stats = dlq.getStatistics();
        assertTrue(stats.getCurrentSize() >= 0, "Negative DLQ size");
        assertTrue(stats.getTotalAdded() >= addCount.get(), "Total added count mismatch");
        assertTrue(stats.getTotalRemoved() >= removeCount.get(), "Total removed count mismatch");
        
        assertTrue(errorCount.get() < THREAD_COUNT * 0.1, "Too many errors: " + errorCount.get());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentStatisticsAccess() throws InterruptedException {
        System.out.println("Testing concurrent statistics access with " + THREAD_COUNT + " threads...");
        
        AtomicInteger statsAccessCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        // Pre-populate DLQ
        for (int i = 0; i < 500; i++) {
            Task task = createTestTask(0, i);
            dlq.addToDeadLetterQueue(task, "Stats test", Instant.now());
        }
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        DeadLetterQueueStatistics stats = dlq.getStatistics();
                        
                        // Verify statistics consistency
                        assertTrue(stats.getCurrentSize() >= 0, "Negative current size");
                        assertTrue(stats.getTotalAdded() >= stats.getCurrentSize(), "Total added < current size");
                        assertTrue(stats.getTotalRemoved() >= 0, "Negative total removed");
                        
                        statsAccessCount.incrementAndGet();
                        
                        // Occasionally add/remove entries to change statistics
                        if (random.nextInt(100) < 10) {
                            Task task = createTestTask(threadId, j);
                            dlq.addToDeadLetterQueue(task, "Stats change", Instant.now());
                        }
                        
                        if (random.nextInt(100) < 5) {
                            List<DeadLetterEntry> entries = dlq.getAllDeadLetterEntries();
                            if (!entries.isEmpty()) {
                                dlq.removeFromDeadLetterQueue(entries.get(0).getTaskId());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                  "Test timed out after " + TEST_DURATION);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("Concurrent statistics access completed:");
        System.out.println("  Stats accesses: " + statsAccessCount.get());
        System.out.println("  Errors: " + errorCount.get());
        System.out.println("  Duration: " + duration + "ms");
        System.out.println("  Throughput: " + (statsAccessCount.get() * 1000.0 / duration) + " ops/sec");
        
        assertTrue(errorCount.get() < THREAD_COUNT * 0.05, "Too many errors: " + errorCount.get());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentCleanupOperations() throws InterruptedException {
        System.out.println("Testing concurrent cleanup operations with " + THREAD_COUNT + " threads...");
        
        AtomicInteger cleanupCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        // Pre-populate DLQ with old entries
        Instant oldTime = Instant.now().minus(Duration.ofDays(2));
        for (int i = 0; i < 1000; i++) {
            Task task = createTestTask(0, i);
            dlq.addToDeadLetterQueue(task, "Old entry", oldTime);
        }
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < OPERATIONS_PER_THREAD / 10; j++) { // Fewer operations for cleanup
                        // Mix cleanup with other operations
                        int operation = random.nextInt(4);
                        
                        switch (operation) {
                            case 0: // Cleanup operation
                                dlq.cleanupOldEntries();
                                cleanupCount.incrementAndGet();
                                break;
                            case 1: // Add new entry
                                Task task = createTestTask(threadId, j);
                                dlq.addToDeadLetterQueue(task, "Cleanup test", Instant.now());
                                break;
                            case 2: // Get statistics
                                dlq.getStatistics();
                                break;
                            case 3: // Get all entries
                                dlq.getAllDeadLetterEntries();
                                break;
                        }
                        
                        // Small delay
                        if (random.nextInt(100) < 20) {
                            Thread.sleep(random.nextInt(5));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                  "Test timed out after " + TEST_DURATION);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("Concurrent cleanup operations completed:");
        System.out.println("  Cleanups: " + cleanupCount.get());
        System.out.println("  Errors: " + errorCount.get());
        System.out.println("  Duration: " + duration + "ms");
        
        // Verify cleanup worked
        DeadLetterQueueStatistics stats = dlq.getStatistics();
        System.out.println("  Final DLQ size: " + stats.getCurrentSize());
        
        assertTrue(errorCount.get() < THREAD_COUNT * 0.1, "Too many errors: " + errorCount.get());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentCapacityLimit() throws InterruptedException {
        System.out.println("Testing concurrent capacity limit enforcement...");
        
        // Create DLQ with small capacity
        String tmpDir = System.getProperty("java.io.tmpdir");
        String uniqueName = UUID.randomUUID().toString();
        String path = tmpDir.endsWith("/") ? (tmpDir + "dlq-capacity-" + uniqueName + ".db")
                                          : (tmpDir + "/dlq-capacity-" + uniqueName + ".db");
        
        DeadLetterQueue smallDlq = new MapDBDeadLetterQueue(path, 100, true, 30);
        
        try {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
            
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                            Task task = createTestTask(threadId, j);
                            boolean success = smallDlq.addToDeadLetterQueue(task, "Capacity test", Instant.now());
                            if (success) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                        failureCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                      "Test timed out after " + TEST_DURATION);
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            System.out.println("Concurrent capacity limit test completed:");
            System.out.println("  Successes: " + successCount.get());
            System.out.println("  Failures: " + failureCount.get());
            System.out.println("  Duration: " + duration + "ms");
            
            // Verify capacity limit was enforced
            DeadLetterQueueStatistics stats = smallDlq.getStatistics();
            assertTrue(stats.getCurrentSize() <= 100, "Capacity limit exceeded: " + stats.getCurrentSize());
            assertTrue(failureCount.get() > 0, "No capacity limit failures occurred");
            
            System.out.println("  Final DLQ size: " + stats.getCurrentSize());
            System.out.println("  Capacity limit enforced: " + (stats.getCurrentSize() <= 100));
            
        } finally {
            smallDlq.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testConcurrentPaginationAccess() throws InterruptedException {
        System.out.println("Testing concurrent pagination access...");
        
        AtomicInteger paginationCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        // Pre-populate DLQ
        for (int i = 0; i < 2000; i++) {
            Task task = createTestTask(0, i);
            dlq.addToDeadLetterQueue(task, "Pagination test", Instant.now());
        }
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < OPERATIONS_PER_THREAD / 5; j++) { // Fewer operations for pagination
                        int offset = random.nextInt(1000);
                        int limit = random.nextInt(100) + 1;
                        
                        List<DeadLetterEntry> entries = dlq.getDeadLetterEntries(offset, limit);
                        
                        // Verify pagination consistency
                        assertTrue(entries.size() <= limit, "Returned more entries than limit");
                        assertTrue(offset >= 0, "Negative offset");
                        
                        paginationCount.incrementAndGet();
                        
                        // Occasionally add entries to change the dataset
                        if (random.nextInt(100) < 10) {
                            Task task = createTestTask(threadId, j);
                            dlq.addToDeadLetterQueue(task, "Pagination change", Instant.now());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(TEST_DURATION.toMillis(), TimeUnit.MILLISECONDS), 
                  "Test timed out after " + TEST_DURATION);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("Concurrent pagination access completed:");
        System.out.println("  Pagination accesses: " + paginationCount.get());
        System.out.println("  Errors: " + errorCount.get());
        System.out.println("  Duration: " + duration + "ms");
        System.out.println("  Throughput: " + (paginationCount.get() * 1000.0 / duration) + " ops/sec");
        
        assertTrue(errorCount.get() < THREAD_COUNT * 0.05, "Too many errors: " + errorCount.get());
    }

    private Task createTestTask(int threadId, int operationId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("threadId", threadId);
        payload.put("operationId", operationId);
        payload.put("timestamp", System.currentTimeMillis());
        
        return TaskImpl.builder()
                .type("concurrency-test-task")
                .payload(payload)
                .maxRetries(3)
                .retryCount(0)
                .build();
    }
}