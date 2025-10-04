package com.enterprise.taskexecution.dlq;

import com.enterprise.taskexecution.core.Task;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * MapDB-based implementation of Dead Letter Queue.
 */
public class MapDBDeadLetterQueue implements DeadLetterQueue {
    
    private static final Logger logger = LoggerFactory.getLogger(MapDBDeadLetterQueue.class);
    
    private final DB db;
    private final Map<UUID, String> dlqStorage;
    private final Map<UUID, Long> dlqTimestamps;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock;
    private final int maxCapacity;
    private final boolean enableRetentionPolicy;
    private final long retentionDays;
    
    // Statistics tracking
    private final AtomicInteger totalAdded = new AtomicInteger(0);
    private final AtomicInteger totalRemoved = new AtomicInteger(0);
    
    public MapDBDeadLetterQueue(String dbPath, int maxCapacity, boolean enableRetentionPolicy, long retentionDays) {
        this.maxCapacity = maxCapacity;
        this.enableRetentionPolicy = enableRetentionPolicy;
        this.retentionDays = retentionDays;
        this.lock = new ReentrantReadWriteLock();
        
        // Initialize ObjectMapper with JavaTimeModule
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        
        // Initialize MapDB
        this.db = DBMaker.fileDB(new File(dbPath))
            .fileMmapEnable()
            .fileMmapPreclearDisable()
            .allocateStartSize(16 * 1024 * 1024) // 16MB
            .allocateIncrement(8 * 1024 * 1024)  // 8MB
            .transactionEnable()
            .checksumHeaderBypass()
            .closeOnJvmShutdown() // Close on JVM shutdown
            .make();
        
        // Initialize collections
        this.dlqStorage = db.hashMap("dlqStorage", Serializer.UUID, Serializer.STRING).createOrOpen();
        this.dlqTimestamps = db.hashMap("dlqTimestamps", Serializer.UUID, Serializer.LONG).createOrOpen();
        
        logger.info("MapDB Dead Letter Queue initialized with capacity: {}, retention: {} days", 
                   maxCapacity, retentionDays);
    }
    
    @Override
    public boolean addToDeadLetterQueue(Task task, String failureReason, Instant lastAttemptTime) {
        lock.writeLock().lock();
        try {
            // Check capacity
            if (isAtCapacity()) {
                logger.warn("Dead Letter Queue is at capacity ({}), cannot add task {}", maxCapacity, task.getId());
                return false;
            }
            
            // Create DLQ entry
            DeadLetterEntry entry = DeadLetterEntry.create(task, failureReason, lastAttemptTime, 
                                                         task.getRetryCount(), null);
            
            // Serialize and store
            String serializedEntry = objectMapper.writeValueAsString(entry);
            dlqStorage.put(task.getId(), serializedEntry);
            dlqTimestamps.put(task.getId(), entry.getAddedToDlqTime().toEpochMilli());
            
            // Update statistics
            totalAdded.incrementAndGet();
            
            // Commit transaction
            db.commit();
            
            logger.info("Task {} added to Dead Letter Queue. Reason: {}", task.getId(), failureReason);
            return true;
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize task {} for DLQ", task.getId(), e);
            db.rollback();
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error adding task {} to DLQ", task.getId(), e);
            db.rollback();
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public Optional<DeadLetterEntry> getDeadLetterEntry(UUID taskId) {
        lock.readLock().lock();
        try {
            String serializedEntry = dlqStorage.get(taskId);
            if (serializedEntry == null) {
                return Optional.empty();
            }
            
            DeadLetterEntry entry = objectMapper.readValue(serializedEntry, DeadLetterEntry.class);
            return Optional.of(entry);
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize DLQ entry for task {}", taskId, e);
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public List<DeadLetterEntry> getAllDeadLetterEntries() {
        lock.readLock().lock();
        try {
            return dlqStorage.values().stream()
                .map(this::deserializeEntry)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public List<DeadLetterEntry> getDeadLetterEntries(int offset, int limit) {
        lock.readLock().lock();
        try {
            return dlqStorage.values().stream()
                .skip(offset)
                .limit(limit)
                .map(this::deserializeEntry)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean removeFromDeadLetterQueue(UUID taskId) {
        lock.writeLock().lock();
        try {
            String removed = dlqStorage.remove(taskId);
            dlqTimestamps.remove(taskId);
            
            if (removed != null) {
                totalRemoved.incrementAndGet();
                db.commit();
                logger.info("Task {} removed from Dead Letter Queue", taskId);
                return true;
            }
            
            return false;
            
        } catch (Exception e) {
            logger.error("Error removing task {} from DLQ", taskId, e);
            db.rollback();
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public int clearDeadLetterQueue() {
        lock.writeLock().lock();
        try {
            int size = dlqStorage.size();
            dlqStorage.clear();
            dlqTimestamps.clear();
            
            totalRemoved.addAndGet(size);
            db.commit();
            
            logger.info("Cleared {} entries from Dead Letter Queue", size);
            return size;
            
        } catch (Exception e) {
            logger.error("Error clearing Dead Letter Queue", e);
            db.rollback();
            return 0;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public int getDeadLetterQueueSize() {
        lock.readLock().lock();
        try {
            return dlqStorage.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isAtCapacity() {
        return getDeadLetterQueueSize() >= maxCapacity;
    }
    
    @Override
    public DeadLetterQueueStatistics getStatistics() {
        lock.readLock().lock();
        try {
            List<DeadLetterEntry> entries = getAllDeadLetterEntries();
            
            Map<String, Integer> errorTypeCounts = entries.stream()
                .collect(Collectors.groupingBy(
                    DeadLetterEntry::getErrorType,
                    Collectors.collectingAndThen(Collectors.counting(), Math::toIntExact)
                ));
            
            Map<String, Integer> taskTypeCounts = entries.stream()
                .collect(Collectors.groupingBy(
                    entry -> entry.getOriginalTask().getType(),
                    Collectors.collectingAndThen(Collectors.counting(), Math::toIntExact)
                ));
            
            Instant oldestTime = entries.stream()
                .map(DeadLetterEntry::getAddedToDlqTime)
                .min(Instant::compareTo)
                .orElse(null);
            
            Instant newestTime = entries.stream()
                .map(DeadLetterEntry::getAddedToDlqTime)
                .max(Instant::compareTo)
                .orElse(null);
            
            return new DeadLetterQueueStatistics(
                getDeadLetterQueueSize(),
                totalAdded.get(),
                totalRemoved.get(),
                oldestTime,
                newestTime,
                errorTypeCounts,
                taskTypeCounts
            );
            
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Cleanup old entries based on retention policy.
     */
    public void cleanupOldEntries() {
        if (!enableRetentionPolicy) {
            return;
        }
        
        lock.writeLock().lock();
        try {
            Instant cutoffTime = Instant.now().minusSeconds(retentionDays * 24 * 60 * 60);
            List<UUID> toRemove = new ArrayList<>();
            
            for (Map.Entry<UUID, Long> entry : dlqTimestamps.entrySet()) {
                if (Instant.ofEpochMilli(entry.getValue()).isBefore(cutoffTime)) {
                    toRemove.add(entry.getKey());
                }
            }
            
            for (UUID taskId : toRemove) {
                dlqStorage.remove(taskId);
                dlqTimestamps.remove(taskId);
                totalRemoved.incrementAndGet();
            }
            
            if (!toRemove.isEmpty()) {
                db.commit();
                logger.info("Cleaned up {} old entries from Dead Letter Queue", toRemove.size());
            }
            
        } catch (Exception e) {
            logger.error("Error cleaning up old DLQ entries", e);
            db.rollback();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Close the DLQ and release resources.
     */
    public void close() {
        try {
            db.close();
            logger.info("Dead Letter Queue closed");
        } catch (Exception e) {
            logger.error("Error closing Dead Letter Queue", e);
        }
    }
    
    private Optional<DeadLetterEntry> deserializeEntry(String serializedEntry) {
        try {
            DeadLetterEntry entry = objectMapper.readValue(serializedEntry, DeadLetterEntry.class);
            return Optional.of(entry);
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize DLQ entry", e);
            return Optional.empty();
        }
    }
}