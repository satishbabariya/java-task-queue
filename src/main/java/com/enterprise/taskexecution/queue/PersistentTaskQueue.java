package com.enterprise.taskexecution.queue;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * MapDB-based persistent task queue implementation.
 * Provides thread-safe, persistent storage for tasks with transaction support.
 */
public class PersistentTaskQueue {
    
    private static final Logger logger = LoggerFactory.getLogger(PersistentTaskQueue.class);
    
    private final DB db;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // MapDB collections
    private final Map<UUID, String> taskStorage;
    private final Map<UUID, String> taskStatuses;
    private final Map<UUID, Long> taskPriorities;
    private final Map<UUID, Long> taskScheduledTimes;
    
    // In-memory indexes for fast access
    private final Map<TaskStatus, Set<UUID>> statusIndex = new ConcurrentHashMap<>();
    private final Map<String, Set<UUID>> typeIndex = new ConcurrentHashMap<>();
    private final PriorityQueue<UUID> priorityQueue = new PriorityQueue<>(
        Comparator.comparingLong(this::getTaskPriority).reversed()
    );
    
    public PersistentTaskQueue(String dbPath) {
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
        
        // Initialize MapDB
        this.db = DBMaker.fileDB(new File(dbPath))
            .fileMmapEnable()
            .fileMmapPreclearDisable()
            .allocateStartSize(64 * 1024 * 1024) // 64MB
            .allocateIncrement(16 * 1024 * 1024) // 16MB
            .transactionEnable()
            .checksumHeaderBypass()
            .closeOnJvmShutdown() // Close on JVM shutdown
            .make();
        
        // Initialize collections
        this.taskStorage = db.hashMap("tasks", Serializer.UUID, Serializer.STRING).createOrOpen();
        this.taskStatuses = db.hashMap("taskStatuses", Serializer.UUID, Serializer.STRING).createOrOpen();
        this.taskPriorities = db.hashMap("taskPriorities", Serializer.UUID, Serializer.LONG).createOrOpen();
        this.taskScheduledTimes = db.hashMap("taskScheduledTimes", Serializer.UUID, Serializer.LONG).createOrOpen();
        
        // Initialize indexes
        initializeIndexes();
        
        logger.info("PersistentTaskQueue initialized with database at: {}", dbPath);
    }
    
    private void initializeIndexes() {
        lock.writeLock().lock();
        try {
            // Initialize status index
            for (TaskStatus status : TaskStatus.values()) {
                statusIndex.put(status, ConcurrentHashMap.newKeySet());
            }
            
            // Rebuild indexes from persistent storage
            for (UUID taskId : taskStorage.keySet()) {
                TaskStatus status = TaskStatus.valueOf(taskStatuses.get(taskId));
                statusIndex.get(status).add(taskId);
                
                // Add to priority queue if pending
                if (status == TaskStatus.PENDING) {
                    priorityQueue.offer(taskId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Add a task to the queue
     */
    public void enqueue(Task task) {
        lock.writeLock().lock();
        try {
            UUID taskId = task.getId();
            
            // Serialize and store task
            String taskJson = objectMapper.writeValueAsString(task);
            taskStorage.put(taskId, taskJson);
            taskStatuses.put(taskId, TaskStatus.PENDING.name());
            taskPriorities.put(taskId, (long) task.getPriority());
            
            if (task.getScheduledFor() != null) {
                taskScheduledTimes.put(taskId, task.getScheduledFor().toEpochMilli());
            }
            
            // Update indexes
            statusIndex.get(TaskStatus.PENDING).add(taskId);
            updateTypeIndex(task);
            
            // Add to priority queue if immediate execution
            if (task.getScheduledFor() == null || task.getScheduledFor().isBefore(Instant.now())) {
                priorityQueue.offer(taskId);
            }
            
            db.commit();
            logger.debug("Task {} enqueued successfully", taskId);
            
        } catch (Exception e) {
            db.rollback();
            logger.error("Failed to enqueue task {}", task.getId(), e);
            throw new RuntimeException("Failed to enqueue task", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Remove and return the highest priority task
     */
    public Optional<Task> dequeue() {
        lock.writeLock().lock();
        try {
            UUID taskId = priorityQueue.poll();
            if (taskId == null) {
                return Optional.empty();
            }
            
            // Check if task is still pending
            if (!statusIndex.get(TaskStatus.PENDING).contains(taskId)) {
                return dequeue(); // Recursively try next task
            }
            
            // Update status
            taskStatuses.put(taskId, TaskStatus.RUNNING.name());
            statusIndex.get(TaskStatus.PENDING).remove(taskId);
            statusIndex.get(TaskStatus.RUNNING).add(taskId);
            
            // Deserialize and return task
            String taskJson = taskStorage.get(taskId);
            Task task = objectMapper.readValue(taskJson, com.enterprise.taskexecution.core.TaskImpl.class);
            
            db.commit();
            logger.debug("Task {} dequeued successfully", taskId);
            
            return Optional.of(task);
            
        } catch (Exception e) {
            db.rollback();
            logger.error("Failed to dequeue task", e);
            throw new RuntimeException("Failed to dequeue task", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Update task status
     */
    public void updateTaskStatus(UUID taskId, TaskStatus status) {
        lock.writeLock().lock();
        try {
            String oldStatusStr = taskStatuses.get(taskId);
            TaskStatus oldStatus = oldStatusStr != null ? TaskStatus.valueOf(oldStatusStr) : null;
            
            taskStatuses.put(taskId, status.name());
            if (oldStatus != null) {
                statusIndex.get(oldStatus).remove(taskId);
            }
            statusIndex.get(status).add(taskId);
            
            // Remove from priority queue if no longer pending
            if (status != TaskStatus.PENDING) {
                priorityQueue.remove(taskId);
            }
            
            db.commit();
            logger.debug("Task {} status updated from {} to {}", taskId, oldStatus, status);
            
        } catch (Exception e) {
            db.rollback();
            logger.error("Failed to update task status for {}", taskId, e);
            throw new RuntimeException("Failed to update task status", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get tasks by status
     */
    public List<Task> getTasksByStatus(TaskStatus status) {
        lock.readLock().lock();
        try {
            return statusIndex.get(status).stream()
                .map(this::getTaskById)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get task by ID
     */
    public Optional<Task> getTaskById(UUID taskId) {
        lock.readLock().lock();
        try {
            String taskJson = taskStorage.get(taskId);
            if (taskJson == null) {
                return Optional.empty();
            }
            
            Task task = objectMapper.readValue(taskJson, com.enterprise.taskexecution.core.TaskImpl.class);
            return Optional.of(task);
            
        } catch (Exception e) {
            logger.error("Failed to get task {}", taskId, e);
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get current queue size
     */
    public int size() {
        lock.readLock().lock();
        try {
            return statusIndex.get(TaskStatus.PENDING).size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Check if queue is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }
    
    /**
     * Get tasks scheduled for execution
     */
    public List<Task> getScheduledTasks(Instant now) {
        lock.readLock().lock();
        try {
            return taskScheduledTimes.entrySet().stream()
                .filter(entry -> entry.getValue() <= now.toEpochMilli())
                .map(Map.Entry::getKey)
                .filter(taskId -> statusIndex.get(TaskStatus.PENDING).contains(taskId))
                .map(this::getTaskById)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Remove task from queue
     */
    public boolean removeTask(UUID taskId) {
        lock.writeLock().lock();
        try {
            if (!taskStorage.containsKey(taskId)) {
                return false;
            }
            
            TaskStatus status = TaskStatus.valueOf(taskStatuses.get(taskId));
            
            // Remove from storage
            taskStorage.remove(taskId);
            taskStatuses.remove(taskId);
            taskPriorities.remove(taskId);
            taskScheduledTimes.remove(taskId);
            
            // Update indexes
            statusIndex.get(status).remove(taskId);
            priorityQueue.remove(taskId);
            removeFromTypeIndex(taskId);
            
            db.commit();
            logger.debug("Task {} removed from queue", taskId);
            
            return true;
            
        } catch (Exception e) {
            db.rollback();
            logger.error("Failed to remove task {}", taskId, e);
            throw new RuntimeException("Failed to remove task", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Close the queue and database
     */
    public void close() {
        lock.writeLock().lock();
        try {
            db.close();
            logger.info("PersistentTaskQueue closed");
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private void updateTypeIndex(Task task) {
        typeIndex.computeIfAbsent(task.getType(), k -> ConcurrentHashMap.newKeySet())
            .add(task.getId());
    }
    
    private void removeFromTypeIndex(UUID taskId) {
        typeIndex.values().forEach(set -> set.remove(taskId));
    }
    
    private long getTaskPriority(UUID taskId) {
        Long priority = taskPriorities.get(taskId);
        return priority != null ? priority : 0L;
    }
}