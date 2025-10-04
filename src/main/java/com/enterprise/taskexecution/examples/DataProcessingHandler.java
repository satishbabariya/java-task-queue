package com.enterprise.taskexecution.examples;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskHandler;
import com.enterprise.taskexecution.core.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Example task handler for data processing
 */
public class DataProcessingHandler implements TaskHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(DataProcessingHandler.class);
    
    @Override
    public CompletableFuture<TaskResult> handle(Task task) {
        long startTime = System.currentTimeMillis();
        
        try {
            Map<String, Object> payload = task.getPayload();
            
            String dataSource = (String) payload.get("dataSource");
            String operation = (String) payload.get("operation");
            Integer batchSize = (Integer) payload.getOrDefault("batchSize", 1000);
            Boolean validateData = (Boolean) payload.getOrDefault("validateData", true);
            
            logger.info("Processing data from {} with operation: {}", dataSource, operation);
            
            // Simulate data processing
            ProcessingResult result = processData(dataSource, operation, batchSize, validateData);
            
            Map<String, Object> resultData = Map.of(
                "status", "completed",
                "dataSource", dataSource,
                "operation", operation,
                "recordsProcessed", result.recordsProcessed,
                "recordsValid", result.recordsValid,
                "recordsInvalid", result.recordsInvalid,
                "processingTimeMs", result.processingTimeMs,
                "timestamp", System.currentTimeMillis()
            );
            
            long executionTime = System.currentTimeMillis() - startTime;
            logger.info("Data processing completed for {} in {}ms", dataSource, executionTime);
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, executionTime)
            );
            
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Data processing failed for task {}", task.getId(), e);
            
            return CompletableFuture.completedFuture(
                TaskResult.failure("Data processing failed: " + e.getMessage(), e, true, executionTime)
            );
        }
    }
    
    @Override
    public String getSupportedTaskType() {
        return "data-processing";
    }
    
    private ProcessingResult processData(String dataSource, String operation, 
                                       int batchSize, boolean validateData) throws Exception {
        
        // Simulate data processing time based on batch size
        int processingTime = batchSize / 100 + ThreadLocalRandom.current().nextInt(100, 500);
        Thread.sleep(processingTime);
        
        // Simulate processing results
        int recordsProcessed = batchSize;
        int recordsValid = validateData ? (int)(batchSize * 0.95) : batchSize;
        int recordsInvalid = batchSize - recordsValid;
        
        // Simulate occasional failures
        if (ThreadLocalRandom.current().nextDouble() < 0.05) { // 5% failure rate
            throw new RuntimeException("Data validation failed");
        }
        
        return new ProcessingResult(recordsProcessed, recordsValid, recordsInvalid, processingTime);
    }
    
    private static class ProcessingResult {
        final int recordsProcessed;
        final int recordsValid;
        final int recordsInvalid;
        final int processingTimeMs;
        
        ProcessingResult(int recordsProcessed, int recordsValid, int recordsInvalid, int processingTimeMs) {
            this.recordsProcessed = recordsProcessed;
            this.recordsValid = recordsValid;
            this.recordsInvalid = recordsInvalid;
            this.processingTimeMs = processingTimeMs;
        }
    }
}