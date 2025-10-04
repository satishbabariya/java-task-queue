package com.enterprise.taskexecution.examples;

import com.enterprise.taskexecution.core.Task;
import com.enterprise.taskexecution.core.TaskHandler;
import com.enterprise.taskexecution.core.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Example task handler for sending emails
 */
public class EmailTaskHandler implements TaskHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(EmailTaskHandler.class);
    
    @Override
    public CompletableFuture<TaskResult> handle(Task task) {
        long startTime = System.currentTimeMillis();
        
        try {
            Map<String, Object> payload = task.getPayload();
            
            String recipient = (String) payload.get("recipient");
            String subject = (String) payload.get("subject");
            String body = (String) payload.get("body");
            String template = (String) payload.get("template");
            
            // Validate required fields
            if (recipient == null || subject == null || body == null) {
                throw new IllegalArgumentException("Missing required email fields");
            }
            
            logger.info("Sending email to {} with subject: {}", recipient, subject);
            
            // Simulate email sending
            boolean success = sendEmail(recipient, subject, body, template);
            
            if (success) {
                Map<String, Object> result = Map.of(
                    "status", "sent",
                    "recipient", recipient,
                    "subject", subject,
                    "timestamp", System.currentTimeMillis()
                );
                
                long executionTime = System.currentTimeMillis() - startTime;
                logger.info("Email sent successfully to {} in {}ms", recipient, executionTime);
                
                return CompletableFuture.completedFuture(
                    TaskResult.success(result, executionTime)
                );
            } else {
                throw new RuntimeException("Failed to send email");
            }
            
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Failed to send email for task {}", task.getId(), e);
            
            return CompletableFuture.completedFuture(
                TaskResult.failure("Email sending failed: " + e.getMessage(), e, true, executionTime)
            );
        }
    }
    
    @Override
    public String getSupportedTaskType() {
        return "email";
    }
    
    private boolean sendEmail(String recipient, String subject, String body, String template) {
        // Simulate email sending logic
        try {
            // In a real implementation, this would integrate with an email service
            // like SendGrid, AWS SES, or SMTP
            
            Thread.sleep(100 + (int)(Math.random() * 200)); // Simulate network delay
            
            // Simulate occasional failures
            if (Math.random() < 0.1) { // 10% failure rate
                throw new RuntimeException("SMTP server unavailable");
            }
            
            logger.debug("Email sent to {}: {}", recipient, subject);
            return true;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Email sending interrupted", e);
        }
    }
}