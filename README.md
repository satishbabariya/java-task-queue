# Java Task Queue

[![GitHub](https://img.shields.io/badge/GitHub-satishbabariya%2Fjava--task--queue-blue)](https://github.com/satishbabariya/java-task-queue)

A production-grade, enterprise-ready task execution system with async queue capabilities using MapDB as the backend. This library provides a comprehensive job scheduling solution that can be integrated into virtually any Java application.

## Features

- **Persistent Queue**: MapDB-based persistent storage with transaction support
- **Async Execution**: Multi-threaded task execution with configurable thread pools
- **Job Scheduling**: Cron-like expressions and delayed execution
- **Retry Mechanisms**: Configurable retry policies with exponential backoff
- **Monitoring**: Built-in metrics collection and health checks
- **Enterprise Ready**: Production-grade reliability and performance
- **Easy Integration**: Simple API for quick integration into existing applications

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.github.satishbabariya</groupId>
    <artifactId>java-task-queue</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Basic Usage

```java
import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.core.*;

// Create engine with default configuration
TaskExecutionEngine engine = TaskExecutionEngineFactory.createDefault();

// Register a task handler
engine.registerHandler(new MyTaskHandler());

// Start the engine
engine.start();

// Submit a task
Task task = TaskImpl.builder()
    .type("my-task")
    .payload(Map.of("message", "Hello World"))
    .priority(10)
    .build();

CompletableFuture<TaskResult> future = engine.submit(task);
TaskResult result = future.get();

// Stop the engine
engine.stop().get();
```

## Architecture

### Core Components

1. **TaskExecutionEngine**: Main interface for task management
2. **PersistentTaskQueue**: MapDB-based persistent queue
3. **AsyncTaskExecutor**: Multi-threaded task execution
4. **CronScheduler**: Job scheduling with cron expressions
5. **MetricsCollector**: Performance monitoring and metrics
6. **HealthChecker**: System health monitoring

### Task Lifecycle

```
Task Submission → Queue → Execution → Result → Cleanup
     ↓              ↓         ↓         ↓        ↓
   Validation   Persistence  Retry   Metrics   Archive
```

## Configuration

### Default Configuration

```java
TaskExecutionConfig config = TaskExecutionConfig.builder()
    .executorConfig(new TaskExecutionConfig.ExecutorConfig(
        4, 16, Duration.ofMinutes(1), 1000, Duration.ofSeconds(30)
    ))
    .queueConfig(new TaskExecutionConfig.QueueConfig(
        "task-queue.db", true, 100000, Duration.ofHours(1)
    ))
    .retryConfig(new TaskExecutionConfig.RetryConfig(
        3, Duration.ofSeconds(5), 2.0, Duration.ofMinutes(5), true
    ))
    .monitoringConfig(new TaskExecutionConfig.MonitoringConfig(
        true, true, Duration.ofMinutes(1), false
    ))
    .build();

TaskExecutionEngine engine = TaskExecutionEngineFactory.create(config);
```

### Custom Configuration

```java
TaskExecutionConfig customConfig = TaskExecutionConfig.builder()
    .executorConfig(new TaskExecutionConfig.ExecutorConfig(
        8, 32, Duration.ofMinutes(2), 2000, Duration.ofSeconds(60)
    ))
    .queueConfig(new TaskExecutionConfig.QueueConfig(
        "/var/lib/myapp/tasks.db", false, 500000, Duration.ofMinutes(30)
    ))
    .retryConfig(new TaskExecutionConfig.RetryConfig(
        5, Duration.ofSeconds(2), 1.5, Duration.ofMinutes(10), true
    ))
    .customProperty("custom.setting", "value")
    .build();
```

## Task Handlers

### Implementing Task Handlers

```java
public class EmailTaskHandler implements TaskHandler {
    
    @Override
    public CompletableFuture<TaskResult> handle(Task task) {
        Map<String, Object> payload = task.getPayload();
        String recipient = (String) payload.get("recipient");
        String subject = (String) payload.get("subject");
        String body = (String) payload.get("body");
        
        try {
            // Send email logic here
            sendEmail(recipient, subject, body);
            
            Map<String, Object> result = Map.of(
                "status", "sent",
                "recipient", recipient,
                "timestamp", Instant.now()
            );
            
            return CompletableFuture.completedFuture(
                TaskResult.success(result, System.currentTimeMillis() - startTime)
            );
            
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                TaskResult.failure("Failed to send email: " + e.getMessage(), e, true, 0)
            );
        }
    }
    
    @Override
    public String getSupportedTaskType() {
        return "email";
    }
}
```

### Registering Handlers

```java
engine.registerHandler(new EmailTaskHandler());
engine.registerHandler(new DataProcessingHandler());
engine.registerHandler(new ReportGenerationHandler());
```

## Job Scheduling

### Using Cron Scheduler

```java
import com.enterprise.taskexecution.TaskExecutionEngineFactory;
import com.enterprise.taskexecution.scheduler.CronScheduler;

// Create engine with scheduler
TaskExecutionEngineWithScheduler engineWithScheduler = 
    TaskExecutionEngineFactory.createWithScheduler();

TaskExecutionEngine engine = engineWithScheduler.getEngine();
CronScheduler scheduler = engineWithScheduler.getScheduler();

// Start both
engineWithScheduler.start();

// Schedule a recurring task
scheduler.scheduleCron(
    "daily-report",
    "0 0 9 * * *", // Every day at 9 AM
    "report-generation",
    Map.of("reportType", "daily", "format", "pdf"),
    Map.of("priority", "high")
);

// Schedule a one-time task
scheduler.scheduleAt(
    "maintenance-task",
    Instant.now().plusHours(2),
    "maintenance",
    Map.of("action", "cleanup"),
    Map.of("scheduledBy", "system")
);

// Schedule a delayed task
scheduler.scheduleAfter(
    "retry-task",
    300000, // 5 minutes
    "retry-operation",
    Map.of("operationId", "12345"),
    Map.of("retryCount", 1)
);
```

### Cron Expression Examples

```
"0 0 9 * * *"     - Every day at 9 AM
"0 */15 * * * *"  - Every 15 minutes
"0 0 0 1 * *"     - First day of every month at midnight
"0 0 12 * * 1-5"  - Weekdays at noon
"0 30 8-18 * * 1-5" - Every 30 minutes during business hours
```

## Retry Policies

### Predefined Retry Policies

```java
// No retry
RetryPolicy noRetry = RetryPolicy.Predefined.noRetry();

// Quick retry for transient failures
RetryPolicy quickRetry = RetryPolicy.Predefined.quickRetry();

// Standard retry policy
RetryPolicy standard = RetryPolicy.Predefined.standard();

// Aggressive retry for critical tasks
RetryPolicy aggressive = RetryPolicy.Predefined.aggressive();

// Network-specific retry
RetryPolicy networkRetry = RetryPolicy.Predefined.networkRetry();
```

### Custom Retry Policy

```java
RetryPolicy customRetry = RetryPolicy.builder()
    .maxRetries(5)
    .baseDelay(Duration.ofSeconds(3))
    .backoffMultiplier(1.5)
    .maxDelay(Duration.ofMinutes(2))
    .retryableExceptions(ex -> 
        ex instanceof IOException ||
        ex instanceof SocketTimeoutException)
    .build();
```

## Monitoring and Metrics

### Built-in Metrics

The engine provides comprehensive metrics through Micrometer:

- `taskexecution.tasks.submitted` - Total tasks submitted
- `taskexecution.tasks.completed` - Total tasks completed
- `taskexecution.tasks.failed` - Total tasks failed
- `taskexecution.tasks.retried` - Total task retries
- `taskexecution.task.execution.time` - Task execution time
- `taskexecution.queue.size` - Current queue size
- `taskexecution.tasks.running` - Currently running tasks

### Health Checks

```java
import com.enterprise.taskexecution.monitoring.HealthChecker;

HealthChecker healthChecker = new HealthChecker(engine, queue, metricsCollector);

CompletableFuture<HealthChecker.HealthStatus> healthFuture = 
    healthChecker.performHealthCheck();

HealthChecker.HealthStatus health = healthFuture.get();

if (health.isHealthy()) {
    System.out.println("System is healthy");
} else {
    System.out.println("System has issues:");
    health.getChecks().forEach((name, check) -> {
        if (!check.isPassed()) {
            System.out.println("  - " + name + ": " + check.getMessage());
        }
    });
}
```

## Error Handling

### Task Failure Handling

```java
public class RobustTaskHandler implements TaskHandler {
    
    @Override
    public CompletableFuture<TaskResult> handle(Task task) {
        try {
            // Task execution logic
            return processTask(task);
            
        } catch (Exception e) {
            // Determine if task should be retried
            boolean shouldRetry = shouldRetryTask(e);
            
            return CompletableFuture.completedFuture(
                TaskResult.failure(
                    "Task failed: " + e.getMessage(),
                    e,
                    shouldRetry,
                    System.currentTimeMillis() - startTime
                )
            );
        }
    }
    
    private boolean shouldRetryTask(Exception e) {
        // Retry on transient errors
        return e instanceof IOException ||
               e instanceof SocketTimeoutException ||
               e instanceof ConnectException;
    }
}
```

### Dead Letter Queue

```java
FailureHandler failureHandler = FailureHandler.builder()
    .retryPolicy(RetryPolicy.Predefined.standard())
    .deadLetterHandler(task -> {
        // Send to dead letter queue
        deadLetterQueue.add(task);
        logger.error("Task {} sent to dead letter queue", task.getId());
    })
    .failureNotifier(task -> {
        // Notify administrators
        notificationService.sendAlert("Task failed: " + task.getId());
    })
    .build();
```

## Performance Tuning

### Thread Pool Configuration

```java
TaskExecutionConfig config = TaskExecutionConfig.builder()
    .executorConfig(new TaskExecutionConfig.ExecutorConfig(
        16,  // Core pool size
        64,  // Maximum pool size
        Duration.ofMinutes(5), // Keep alive time
        5000, // Queue capacity
        Duration.ofSeconds(60) // Shutdown timeout
    ))
    .build();
```

### Queue Configuration

```java
TaskExecutionConfig config = TaskExecutionConfig.builder()
    .queueConfig(new TaskExecutionConfig.QueueConfig(
        "/fast/ssd/task-queue.db", // Use fast storage
        true,  // Enable compression
        1000000, // Large queue size
        Duration.ofMinutes(15) // Frequent cleanup
    ))
    .build();
```

## Integration Examples

### Spring Boot Integration

```java
@Configuration
public class TaskExecutionConfig {
    
    @Bean
    public TaskExecutionEngine taskExecutionEngine() {
        return TaskExecutionEngineFactory.createDefault();
    }
    
    @Bean
    public CronScheduler cronScheduler(TaskExecutionEngine engine) {
        return new CronScheduler(engine);
    }
}

@Service
public class TaskService {
    
    @Autowired
    private TaskExecutionEngine engine;
    
    public CompletableFuture<TaskResult> submitTask(String type, Map<String, Object> payload) {
        Task task = TaskImpl.builder()
            .type(type)
            .payload(payload)
            .build();
        
        return engine.submit(task);
    }
}
```

### Microservice Integration

```java
@RestController
@RequestMapping("/api/tasks")
public class TaskController {
    
    @Autowired
    private TaskExecutionEngine engine;
    
    @PostMapping("/submit")
    public ResponseEntity<String> submitTask(@RequestBody TaskRequest request) {
        Task task = TaskImpl.builder()
            .type(request.getType())
            .payload(request.getPayload())
            .priority(request.getPriority())
            .build();
        
        CompletableFuture<TaskResult> future = engine.submit(task);
        
        return ResponseEntity.ok(task.getId().toString());
    }
    
    @GetMapping("/status/{taskId}")
    public ResponseEntity<TaskStatus> getTaskStatus(@PathVariable UUID taskId) {
        try {
            TaskStatus status = engine.getTaskStatus(taskId).get();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }
}
```

## Best Practices

### 1. Task Design
- Keep tasks focused and atomic
- Use appropriate timeouts
- Include retry logic in handlers
- Design for idempotency

### 2. Error Handling
- Implement proper exception handling
- Use appropriate retry policies
- Monitor failure rates
- Implement dead letter queues

### 3. Performance
- Tune thread pool sizes based on workload
- Use appropriate queue sizes
- Monitor queue depth
- Implement backpressure

### 4. Monitoring
- Enable metrics collection
- Set up health checks
- Monitor task execution times
- Alert on failures

### 5. Security
- Validate task payloads
- Implement access controls
- Secure task handlers
- Audit task execution

## Troubleshooting

### Common Issues

1. **Tasks not executing**
   - Check if engine is started
   - Verify handlers are registered
   - Check thread pool configuration

2. **High memory usage**
   - Reduce queue size
   - Enable compression
   - Increase cleanup frequency

3. **Slow task execution**
   - Increase thread pool size
   - Check for blocking operations
   - Optimize task handlers

4. **Task failures**
   - Check retry policies
   - Review error logs
   - Implement proper error handling

### Debugging

```java
// Enable debug logging
Logger logger = LoggerFactory.getLogger("com.enterprise.taskexecution");
logger.setLevel(Level.DEBUG);

// Check engine statistics
EngineStatistics stats = engine.getStatistics();
System.out.println("Queue size: " + stats.getQueueSize());
System.out.println("Running tasks: " + stats.getTasksRunning());
System.out.println("Success rate: " + 
    (double) stats.getTotalTasksCompleted() / stats.getTotalTasksSubmitted() * 100);
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository at [https://github.com/satishbabariya/java-task-queue](https://github.com/satishbabariya/java-task-queue)
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For support and questions:
- Create an issue on [GitHub](https://github.com/satishbabariya/java-task-queue/issues)
- Check the documentation
- Review the examples