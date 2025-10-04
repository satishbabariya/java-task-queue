package com.enterprise.taskexecution.monitoring;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for monitoring system resources during stress tests and benchmarks.
 */
public class ResourceMonitor {
    
    private final ScheduledExecutorService scheduler;
    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private final AtomicLong maxMemoryUsed = new AtomicLong(0);
    private final AtomicLong maxThreadCount = new AtomicLong(0);
    private final AtomicLong totalCpuTime = new AtomicLong(0);
    private volatile boolean monitoring = false;
    private Instant startTime;
    
    public ResourceMonitor() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "resource-monitor");
            t.setDaemon(true);
            return t;
        });
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.threadBean = ManagementFactory.getThreadMXBean();
    }
    
    /**
     * Start monitoring system resources at the specified interval.
     */
    public void startMonitoring(long intervalMs) {
        if (monitoring) {
            return;
        }
        
        monitoring = true;
        startTime = Instant.now();
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Monitor memory usage
                MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
                long usedMemory = heapUsage.getUsed();
                long maxMemory = heapUsage.getMax();
                
                maxMemoryUsed.updateAndGet(current -> Math.max(current, usedMemory));
                
                // Monitor thread count
                int threadCount = threadBean.getThreadCount();
                maxThreadCount.updateAndGet(current -> Math.max(current, threadCount));
                
                // Monitor CPU time (if available)
                if (threadBean.isCurrentThreadCpuTimeSupported()) {
                    long cpuTime = threadBean.getCurrentThreadCpuTime();
                    totalCpuTime.addAndGet(cpuTime);
                }
                
            } catch (Exception e) {
                System.err.println("Error monitoring resources: " + e.getMessage());
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Stop monitoring and return resource statistics.
     */
    public ResourceStats stopMonitoring() {
        if (!monitoring) {
            return new ResourceStats();
        }
        
        monitoring = false;
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        Duration monitoringDuration = Duration.between(startTime, Instant.now());
        
        return new ResourceStats(
            maxMemoryUsed.get(),
            maxThreadCount.get(),
            totalCpuTime.get(),
            monitoringDuration
        );
    }
    
    /**
     * Get current resource usage snapshot.
     */
    public ResourceSnapshot getCurrentSnapshot() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        int threadCount = threadBean.getThreadCount();
        
        return new ResourceSnapshot(
            heapUsage.getUsed(),
            heapUsage.getMax(),
            threadCount,
            Instant.now()
        );
    }
    
    /**
     * Force garbage collection and return memory usage.
     */
    public long forceGCAndGetMemoryUsage() {
        System.gc();
        try {
            Thread.sleep(1000); // Wait for GC to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return heapUsage.getUsed();
    }
    
    /**
     * Resource statistics collected during monitoring.
     */
    public static class ResourceStats {
        private final long maxMemoryUsed;
        private final long maxThreadCount;
        private final long totalCpuTime;
        private final Duration monitoringDuration;
        
        public ResourceStats() {
            this(0, 0, 0, Duration.ZERO);
        }
        
        public ResourceStats(long maxMemoryUsed, long maxThreadCount, long totalCpuTime, Duration monitoringDuration) {
            this.maxMemoryUsed = maxMemoryUsed;
            this.maxThreadCount = maxThreadCount;
            this.totalCpuTime = totalCpuTime;
            this.monitoringDuration = monitoringDuration;
        }
        
        public long getMaxMemoryUsed() { return maxMemoryUsed; }
        public long getMaxMemoryUsedMB() { return maxMemoryUsed / 1024 / 1024; }
        public long getMaxThreadCount() { return maxThreadCount; }
        public long getTotalCpuTime() { return totalCpuTime; }
        public Duration getMonitoringDuration() { return monitoringDuration; }
        
        @Override
        public String toString() {
            return String.format(
                "ResourceStats{maxMemory=%dMB, maxThreads=%d, cpuTime=%dns, duration=%dms}",
                getMaxMemoryUsedMB(), maxThreadCount, totalCpuTime, monitoringDuration.toMillis()
            );
        }
    }
    
    /**
     * Snapshot of current resource usage.
     */
    public static class ResourceSnapshot {
        private final long memoryUsed;
        private final long memoryMax;
        private final int threadCount;
        private final Instant timestamp;
        
        public ResourceSnapshot(long memoryUsed, long memoryMax, int threadCount, Instant timestamp) {
            this.memoryUsed = memoryUsed;
            this.memoryMax = memoryMax;
            this.threadCount = threadCount;
            this.timestamp = timestamp;
        }
        
        public long getMemoryUsed() { return memoryUsed; }
        public long getMemoryUsedMB() { return memoryUsed / 1024 / 1024; }
        public long getMemoryMax() { return memoryMax; }
        public long getMemoryMaxMB() { return memoryMax / 1024 / 1024; }
        public double getMemoryUsagePercent() { 
            return memoryMax > 0 ? (double) memoryUsed / memoryMax * 100 : 0; 
        }
        public int getThreadCount() { return threadCount; }
        public Instant getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format(
                "ResourceSnapshot{memory=%dMB/%dMB (%.1f%%), threads=%d, time=%s}",
                getMemoryUsedMB(), getMemoryMaxMB(), getMemoryUsagePercent(), 
                threadCount, timestamp
            );
        }
    }
}