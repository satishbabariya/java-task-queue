package com.enterprise.taskexecution;

import com.enterprise.taskexecution.config.TaskExecutionConfig;
import com.enterprise.taskexecution.core.TaskExecutionEngine;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

class TaskExecutionEngineFactoryTest {
    
    @Test
    void testCreateDefault() {
        TaskExecutionEngine engine = TaskExecutionEngineFactory.createDefault();
        
        assertNotNull(engine);
        assertFalse(engine.isRunning());
    }
    
    @Test
    void testCreateWithCustomConfig() {
        TaskExecutionConfig config = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                2, 8, Duration.ofMinutes(2), 500, Duration.ofSeconds(60)
            ))
            .queueConfig(new TaskExecutionConfig.QueueConfig(
                "test-queue.db", false, 10000, Duration.ofMinutes(30)
            ))
            .build();
        
        TaskExecutionEngine engine = TaskExecutionEngineFactory.create(config);
        
        assertNotNull(engine);
        assertFalse(engine.isRunning());
    }
    
    @Test
    void testCreateWithScheduler() {
        TaskExecutionEngineFactory.TaskExecutionEngineWithScheduler engineWithScheduler = 
            TaskExecutionEngineFactory.createWithScheduler();
        
        assertNotNull(engineWithScheduler.getEngine());
        assertNotNull(engineWithScheduler.getScheduler());
        assertFalse(engineWithScheduler.isRunning());
    }
    
    @Test
    void testCreateWithInvalidConfig() {
        TaskExecutionConfig invalidConfig = TaskExecutionConfig.builder()
            .executorConfig(new TaskExecutionConfig.ExecutorConfig(
                -1, 8, Duration.ofMinutes(2), 500, Duration.ofSeconds(60)
            ))
            .build();
        
        assertThrows(IllegalArgumentException.class, () -> {
            TaskExecutionEngineFactory.create(invalidConfig);
        });
    }
}