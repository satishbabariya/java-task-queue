package com.enterprise.taskexecution.examples;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskHandler;
import com.enterprise.taskexecution.core.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Example task handler for report generation
 */
public class ReportGenerationHandler implements TaskHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(ReportGenerationHandler.class);
    
    @Override
    public CompletableFuture<TaskResult> handle(Task task) {
        long startTime = System.currentTimeMillis();
        
        try {
            Map<String, Object> payload = task.getPayload();
            
            String reportType = (String) payload.get("reportType");
            String format = (String) payload.getOrDefault("format", "pdf");
            String dateRange = (String) payload.getOrDefault("dateRange", "last30days");
            Boolean includeCharts = (Boolean) payload.getOrDefault("includeCharts", true);
            String outputPath = (String) payload.get("outputPath");
            
            logger.info("Generating {} report in {} format", reportType, format);
            
            // Simulate report generation
            ReportResult result = generateReport(reportType, format, dateRange, includeCharts, outputPath);
            
            Map<String, Object> resultData = Map.of(
                "status", "completed",
                "reportType", reportType,
                "format", format,
                "dateRange", dateRange,
                "filePath", result.filePath,
                "fileSize", result.fileSize,
                "pagesGenerated", result.pagesGenerated,
                "generationTimeMs", result.generationTimeMs,
                "timestamp", System.currentTimeMillis()
            );
            
            long executionTime = System.currentTimeMillis() - startTime;
            logger.info("Report generation completed for {} in {}ms", reportType, executionTime);
            
            return CompletableFuture.completedFuture(
                TaskResult.success(resultData, executionTime)
            );
            
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Report generation failed for task {}", task.getId(), e);
            
            return CompletableFuture.completedFuture(
                TaskResult.failure("Report generation failed: " + e.getMessage(), e, true, executionTime)
            );
        }
    }
    
    @Override
    public String getSupportedTaskType() {
        return "report-generation";
    }
    
    private ReportResult generateReport(String reportType, String format, String dateRange, 
                                     boolean includeCharts, String outputPath) throws Exception {
        
        // Simulate report generation time
        int baseTime = 1000; // Base time in ms
        int formatMultiplier = "pdf".equals(format) ? 2 : 1;
        int chartMultiplier = includeCharts ? 3 : 1;
        int typeMultiplier = "financial".equals(reportType) ? 2 : 1;
        
        int generationTime = baseTime * formatMultiplier * chartMultiplier * typeMultiplier;
        generationTime += ThreadLocalRandom.current().nextInt(500, 2000);
        
        Thread.sleep(generationTime);
        
        // Simulate report content generation
        int pagesGenerated = generateReportPages(reportType, dateRange);
        long fileSize = pagesGenerated * 50 * 1024; // ~50KB per page
        
        String fileName = String.format("%s_%s_%s.%s", 
            reportType, 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(Instant.now().atZone(java.time.ZoneId.systemDefault())),
            dateRange,
            format);
        
        String filePath = outputPath != null ? outputPath + "/" + fileName : "/reports/" + fileName;
        
        // Simulate occasional failures
        if (ThreadLocalRandom.current().nextDouble() < 0.03) { // 3% failure rate
            throw new RuntimeException("Report generation failed due to insufficient data");
        }
        
        logger.debug("Generated report: {} ({} pages, {} bytes)", filePath, pagesGenerated, fileSize);
        
        return new ReportResult(filePath, fileSize, pagesGenerated, generationTime);
    }
    
    private int generateReportPages(String reportType, String dateRange) {
        // Simulate different page counts based on report type and date range
        int basePages = switch (reportType) {
            case "financial" -> 20;
            case "sales" -> 15;
            case "inventory" -> 10;
            case "user-activity" -> 25;
            default -> 12;
        };
        
        int rangeMultiplier = switch (dateRange) {
            case "last7days" -> 1;
            case "last30days" -> 2;
            case "last90days" -> 3;
            case "lastyear" -> 4;
            default -> 2;
        };
        
        return basePages * rangeMultiplier + ThreadLocalRandom.current().nextInt(0, 5);
    }
    
    private static class ReportResult {
        final String filePath;
        final long fileSize;
        final int pagesGenerated;
        final int generationTimeMs;
        
        ReportResult(String filePath, long fileSize, int pagesGenerated, int generationTimeMs) {
            this.filePath = filePath;
            this.fileSize = fileSize;
            this.pagesGenerated = pagesGenerated;
            this.generationTimeMs = generationTimeMs;
        }
    }
}