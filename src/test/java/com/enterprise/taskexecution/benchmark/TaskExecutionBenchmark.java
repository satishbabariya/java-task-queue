package com.enterprise.taskexecution.benchmark;

import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.config.TaskExecutionConfig;
import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskHandler;
import com.enterprise.taskexecution.core.TaskResult;
import com.enterprise.taskexecution.core.TaskImpl;
import com.enterprise.taskexecution.core.EngineStatistics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
 * Performance benchmarks for the Task Execution Engine to measure
 * throughput, latency, and resource utilization.
 */
public class TaskExecutionBenchmark {

    private com.enterprise.taskexecution.core.TaskExecutionEngine engine;
    private MeterRegistry meterRegistry;
    private ExecutorService benchmarkExecutor;
    
    // Performance metrics
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    private final List<Long> latencySamples = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> throughputSamples = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        
        // Configure engine for benchmarking
        TaskExecutionConfig config = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                16, 32, java.time.Duration.ofMillis(60000), 2000, java.time.Duration.ofSeconds(30)
            ))
            .queueConfig(new TaskExecutionConfig.QueueConfig(
                "/tmp/benchmark-" + UUID.randomUUID() + ".db", true, 10000, java.time.Duration.ofMinutes(5)
            ))
            .monitoringConfig(new TaskExecutionConfig.MonitoringConfig(
                true, true, java.time.Duration.ofSeconds(5), true
            ))
            .build();

        engine = TaskExecutionEngineFactory.create(config);
        benchmarkExecutor = Executors.newFixedThreadPool(32);
        
        // Register benchmark handlers
        engine.registerHandler(new BenchmarkTaskHandler());
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
        if (benchmarkExecutor != null) {
            benchmarkExecutor.shutdown();
            try {
                if (!benchmarkExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    benchmarkExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Timeout(600) // 10 minutes timeout
    void benchmarkThroughput() throws Exception {
        System.out.println("=== Throughput Benchmark ===");
        
        int[] taskCounts = {100, 500, 1000, 2000, 5000};
        
        for (int taskCount : taskCounts) {
            System.out.printf("\n--- Testing with %d tasks ---%n", taskCount);
            
            // Reset metrics
            completedTasks.set(0);
            failedTasks.set(0);
            totalLatency.set(0);
            latencySamples.clear();
            
            Instant startTime = Instant.now();
            
            // Submit tasks concurrently
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(taskCount);
            
            for (int i = 0; i < taskCount; i++) {
                final int taskIndex = i;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        long taskStartTime = System.nanoTime();
                        
                        Task task = TaskImpl.builder()
                            .type("benchmark-task")
                            .payload(createPayload(taskIndex))
                            .build();
                        
                        CompletableFuture<TaskResult> result = engine.submit(task);
                        TaskResult taskResult = result.get(60, TimeUnit.SECONDS);
                        
                        long taskEndTime = System.nanoTime();
                        long taskLatency = (taskEndTime - taskStartTime) / 1_000_000; // Convert to ms
                        
                        latencySamples.add(taskLatency);
                        totalLatency.addAndGet(taskLatency);
                        
                        if (taskResult.isSuccess()) {
                            completedTasks.incrementAndGet();
                        } else {
                            failedTasks.incrementAndGet();
                        }
                        
                        latch.countDown();
                    } catch (Exception e) {
                        failedTasks.incrementAndGet();
                        latch.countDown();
                    }
                }, benchmarkExecutor);
                
                futures.add(future);
            }
            
            // Wait for all tasks to complete
            assertTrue(latch.await(300, TimeUnit.SECONDS), 
                "All tasks should complete within timeout");
            
            Instant endTime = Instant.now();
            Duration totalDuration = Duration.between(startTime, endTime);
            
            // Calculate metrics
            double throughput = (double) completedTasks.get() / totalDuration.toMillis() * 1000;
            double avgLatency = latencySamples.isEmpty() ? 0 : 
                latencySamples.stream().mapToLong(Long::longValue).average().orElse(0);
            double p95Latency = calculatePercentile(latencySamples, 95);
            double p99Latency = calculatePercentile(latencySamples, 99);
            
            // Print results
            System.out.printf("Tasks: %d%n", taskCount);
            System.out.printf("Completed: %d%n", completedTasks.get());
            System.out.printf("Failed: %d%n", failedTasks.get());
            System.out.printf("Duration: %d ms%n", totalDuration.toMillis());
            System.out.printf("Throughput: %.2f tasks/second%n", throughput);
            System.out.printf("Avg Latency: %.2f ms%n", avgLatency);
            System.out.printf("P95 Latency: %.2f ms%n", p95Latency);
            System.out.printf("P99 Latency: %.2f ms%n", p99Latency);
            
            // Store throughput sample
            throughputSamples.add((long) throughput);
            
            // Validate performance
            assertTrue(completedTasks.get() >= taskCount * 0.95, 
                "At least 95% of tasks should complete");
            assertTrue(throughput > 0, "Throughput should be positive");
            assertTrue(avgLatency < 1000, "Average latency should be reasonable");
        }
        
        // Print summary
        printThroughputSummary();
    }

    @Test
    @Timeout(300)
    void benchmarkLatency() throws Exception {
        System.out.println("=== Latency Benchmark ===");
        
        int taskCount = 1000;
        int warmupTasks = 100;
        
        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < warmupTasks; i++) {
            Task task = TaskImpl.builder()
                .type("benchmark-task")
                .payload(createPayload(i))
                .build();
            
            CompletableFuture<TaskResult> result = engine.submit(task);
            result.get(30, TimeUnit.SECONDS);
        }
        
        // Reset metrics
        completedTasks.set(0);
        latencySamples.clear();
        
        System.out.println("Measuring latency...");
        Instant startTime = Instant.now();
        
        // Submit tasks and measure latency
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < taskCount; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    long taskStartTime = System.nanoTime();
                    
                    Task task = TaskImpl.builder()
                        .type("benchmark-task")
                        .payload(createPayload(taskIndex))
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    TaskResult taskResult = result.get(60, TimeUnit.SECONDS);
                    
                    long taskEndTime = System.nanoTime();
                    long taskLatency = (taskEndTime - taskStartTime) / 1_000_000; // Convert to ms
                    
                    latencySamples.add(taskLatency);
                    
                    if (taskResult.isSuccess()) {
                        completedTasks.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Ignore for latency measurement
                }
            }, benchmarkExecutor);
            
            futures.add(future);
        }
        
        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(300, TimeUnit.SECONDS);
        
        Instant endTime = Instant.now();
        Duration totalDuration = Duration.between(startTime, endTime);
        
        // Calculate latency statistics
        List<Long> sortedLatencies = new ArrayList<>(latencySamples);
        Collections.sort(sortedLatencies);
        
        double avgLatency = sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0);
        double minLatency = sortedLatencies.isEmpty() ? 0 : sortedLatencies.get(0);
        double maxLatency = sortedLatencies.isEmpty() ? 0 : sortedLatencies.get(sortedLatencies.size() - 1);
        double p50Latency = calculatePercentile(sortedLatencies, 50);
        double p90Latency = calculatePercentile(sortedLatencies, 90);
        double p95Latency = calculatePercentile(sortedLatencies, 95);
        double p99Latency = calculatePercentile(sortedLatencies, 99);
        
        // Print results
        System.out.printf("Tasks: %d%n", taskCount);
        System.out.printf("Completed: %d%n", completedTasks.get());
        System.out.printf("Duration: %d ms%n", totalDuration.toMillis());
        System.out.printf("Min Latency: %.2f ms%n", minLatency);
        System.out.printf("Max Latency: %.2f ms%n", maxLatency);
        System.out.printf("Avg Latency: %.2f ms%n", avgLatency);
        System.out.printf("P50 Latency: %.2f ms%n", p50Latency);
        System.out.printf("P90 Latency: %.2f ms%n", p90Latency);
        System.out.printf("P95 Latency: %.2f ms%n", p95Latency);
        System.out.printf("P99 Latency: %.2f ms%n", p99Latency);
        
        // Validate latency
        assertTrue(avgLatency < 500, "Average latency should be reasonable");
        assertTrue(p95Latency < 1000, "P95 latency should be reasonable");
    }

    @Test
    @Timeout(300)
    void benchmarkConcurrency() throws Exception {
        System.out.println("=== Concurrency Benchmark ===");
        
        int[] concurrencyLevels = {1, 5, 10, 20, 50, 100};
        int tasksPerLevel = 200;
        
        for (int concurrency : concurrencyLevels) {
            System.out.printf("\n--- Testing with %d concurrent threads ---%n", concurrency);
            
            // Reset metrics
            completedTasks.set(0);
            failedTasks.set(0);
            latencySamples.clear();
            
            Instant startTime = Instant.now();
            
            // Create executor with specific concurrency level
            ExecutorService concurrencyExecutor = Executors.newFixedThreadPool(concurrency);
            
            try {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                
                for (int i = 0; i < tasksPerLevel; i++) {
                    final int taskIndex = i;
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            long taskStartTime = System.nanoTime();
                            
                            Task task = TaskImpl.builder()
                                .type("benchmark-task")
                                .payload(createPayload(taskIndex))
                                .build();
                            
                            CompletableFuture<TaskResult> result = engine.submit(task);
                            TaskResult taskResult = result.get(60, TimeUnit.SECONDS);
                            
                            long taskEndTime = System.nanoTime();
                            long taskLatency = (taskEndTime - taskStartTime) / 1_000_000;
                            
                            latencySamples.add(taskLatency);
                            
                            if (taskResult.isSuccess()) {
                                completedTasks.incrementAndGet();
                            } else {
                                failedTasks.incrementAndGet();
                            }
                        } catch (Exception e) {
                            failedTasks.incrementAndGet();
                        }
                    }, concurrencyExecutor);
                    
                    futures.add(future);
                }
                
                // Wait for all tasks to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(180, TimeUnit.SECONDS);
                
            } finally {
                concurrencyExecutor.shutdown();
                try {
                    if (!concurrencyExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                        concurrencyExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            Instant endTime = Instant.now();
            Duration totalDuration = Duration.between(startTime, endTime);
            
            // Calculate metrics
            double throughput = (double) completedTasks.get() / totalDuration.toMillis() * 1000;
            double avgLatency = latencySamples.isEmpty() ? 0 : 
                latencySamples.stream().mapToLong(Long::longValue).average().orElse(0);
            
            // Print results
            System.out.printf("Concurrency: %d%n", concurrency);
            System.out.printf("Tasks: %d%n", tasksPerLevel);
            System.out.printf("Completed: %d%n", completedTasks.get());
            System.out.printf("Failed: %d%n", failedTasks.get());
            System.out.printf("Duration: %d ms%n", totalDuration.toMillis());
            System.out.printf("Throughput: %.2f tasks/second%n", throughput);
            System.out.printf("Avg Latency: %.2f ms%n", avgLatency);
            
            // Validate performance
            assertTrue(completedTasks.get() >= tasksPerLevel * 0.9, 
                "At least 90% of tasks should complete");
        }
    }

    @Test
    @Timeout(300)
    void benchmarkMemoryUsage() throws Exception {
        System.out.println("=== Memory Usage Benchmark ===");
        
        Runtime runtime = Runtime.getRuntime();
        
        // Force garbage collection
        System.gc();
        Thread.sleep(1000);
        
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("Initial Memory Usage: %d MB%n", initialMemory / 1024 / 1024);
        
        int taskCount = 1000;
        Instant startTime = Instant.now();
        
        // Submit tasks and monitor memory
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < taskCount; i++) {
            final int taskIndex = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Task task = TaskImpl.builder()
                        .type("benchmark-task")
                        .payload(createLargePayload(taskIndex))
                        .build();
                    
                    CompletableFuture<TaskResult> result = engine.submit(task);
                    result.get(60, TimeUnit.SECONDS);
                    completedTasks.incrementAndGet();
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                }
            }, benchmarkExecutor);
            
            futures.add(future);
            
            // Monitor memory every 100 tasks
            if (i % 100 == 0) {
                long currentMemory = runtime.totalMemory() - runtime.freeMemory();
                System.out.printf("Memory at task %d: %d MB%n", i, currentMemory / 1024 / 1024);
            }
        }
        
        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(300, TimeUnit.SECONDS);
        
        Instant endTime = Instant.now();
        Duration totalDuration = Duration.between(startTime, endTime);
        
        // Final memory check
        System.gc();
        Thread.sleep(1000);
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = finalMemory - initialMemory;
        
        System.out.printf("Final Memory Usage: %d MB%n", finalMemory / 1024 / 1024);
        System.out.printf("Memory Increase: %d MB%n", memoryIncrease / 1024 / 1024);
        System.out.printf("Memory per Task: %.2f KB%n", (double) memoryIncrease / taskCount / 1024);
        
        // Validate memory usage
        assertTrue(memoryIncrease < 100 * 1024 * 1024, // Less than 100MB increase
            "Memory increase should be reasonable");
        assertTrue(completedTasks.get() >= taskCount * 0.95, 
            "At least 95% of tasks should complete");
    }

    private Map<String, Object> createPayload(int index) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("index", index);
        payload.put("timestamp", System.currentTimeMillis());
        payload.put("data", "benchmark-data-" + index);
        return payload;
    }

    private Map<String, Object> createLargePayload(int index) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("index", index);
        payload.put("timestamp", System.currentTimeMillis());
        
        // Create larger payload to test memory usage
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeData.append("large-data-chunk-").append(i).append("-");
        }
        payload.put("largeData", largeData.toString());
        
        return payload;
    }

    private double calculatePercentile(List<Long> values, double percentile) {
        if (values.isEmpty()) return 0;
        
        Collections.sort(values);
        int index = (int) Math.ceil((percentile / 100.0) * values.size()) - 1;
        index = Math.max(0, Math.min(index, values.size() - 1));
        return values.get(index);
    }

    private void printThroughputSummary() {
        System.out.println("\n=== Throughput Summary ===");
        if (!throughputSamples.isEmpty()) {
            double avgThroughput = throughputSamples.stream().mapToLong(Long::longValue).average().orElse(0);
            long maxThroughput = throughputSamples.stream().mapToLong(Long::longValue).max().orElse(0);
            long minThroughput = throughputSamples.stream().mapToLong(Long::longValue).min().orElse(0);
            
            System.out.printf("Average Throughput: %.2f tasks/second%n", avgThroughput);
            System.out.printf("Max Throughput: %d tasks/second%n", maxThroughput);
            System.out.printf("Min Throughput: %d tasks/second%n", minThroughput);
        }
    }

    // Benchmark task handler
    private static class BenchmarkTaskHandler implements TaskHandler {
        @Override
        public CompletableFuture<TaskResult> handle(Task task) {
            // Simulate some work
            try {
                Thread.sleep(10 + new Random().nextInt(20)); // 10-30ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("taskId", task.getId().toString());
            resultData.put("processedAt", System.currentTimeMillis());
            resultData.put("handler", "benchmark-handler");
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, 20L)
            );
        }

        @Override
        public String getSupportedTaskType() {
            return "benchmark-task";
        }
    }
}