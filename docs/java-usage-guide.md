# DelayedQueue: Java Developer Guide

A comprehensive guide for Java developers using the Funfix DelayedQueue library.

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Basic Usage](#basic-usage)
4. [Real-World Scenarios](#real-world-scenarios)
   - [Scenario 1: HTTP Server with Business Hours](#scenario-1-http-server-with-business-hours)
   - [Scenario 2: Daily Cron Job with Multi-Node Processing](#scenario-2-daily-cron-job-with-multi-node-processing)
5. [Best Practices](#best-practices)

## Introduction

DelayedQueue is a high-performance FIFO queue backed by your favorite RDBMS. It enables you to:

- **Schedule messages** for future delivery at specific times
- **Poll with acknowledgement** - unacknowledged messages are automatically redelivered
- **Batch operations** for efficient bulk scheduling
- **Cron-like scheduling** for periodic tasks
- **Multi-node coordination** - multiple instances can share the same queue safely

Supported databases: H2, HSQLDB, MariaDB, Microsoft SQL Server, PostgreSQL, SQLite

## Getting Started

### Add the Dependency

**Maven:**
```xml
<dependency>
  <groupId>org.funfix</groupId>
  <artifactId>delayedqueue-jvm</artifactId>
  <version>0.2.0</version>
</dependency>
```

**Gradle:**
```kotlin
dependencies {
    implementation("org.funfix:delayedqueue-jvm:0.2.0")
}
```

### SQLite Setup

SQLite is perfect for getting started - it requires no external database server.

```java
import org.funfix.delayedqueue.jvm.*;
import java.time.Instant;
import java.time.Duration;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // 1. Configure the database connection
        JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
            "jdbc:sqlite:/tmp/myapp.db",  // Database file path
            JdbcDriver.Sqlite,             // Database driver
            null,                          // Username (not needed for SQLite)
            null,                          // Password (not needed for SQLite)
            null                           // Connection pool config (optional)
        );

        // 2. Configure the queue
        DelayedQueueJDBCConfig queueConfig = DelayedQueueJDBCConfig.create(
            dbConfig,
            "delayed_queue",               // Table name
            "my-queue"                     // Queue name (for partitioning)
        );

        // 3. Run migrations (do this once, typically on application startup)
        DelayedQueueJDBC.runMigrations(queueConfig);

        // 4. Create the queue (implements AutoCloseable)
        try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
            MessageSerializer.forStrings(),
            queueConfig
        )) {
            // Use the queue...
            System.out.println("DelayedQueue is ready!");
        }
    }
}
```

**Important**: Always use try-with-resources or explicitly call `close()` on the DelayedQueue to properly release database connections.

## Basic Usage

### Offering Messages

Schedule a message for future processing:

```java
import java.time.Instant;
import java.time.Duration;

try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig
)) {
    // Schedule a message for 1 hour from now
    Instant deliveryTime = Instant.now().plus(Duration.ofHours(1));
    
    OfferOutcome outcome = queue.offerOrUpdate(
        "transaction-12345",           // Unique key
        "Process payment for order",   // Payload
        deliveryTime                   // When to deliver
    );
    
    System.out.println("Message scheduled: " + outcome);
}
```

### Polling Messages

Retrieve and process messages:

```java
try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig
)) {
    // Try to poll a message (returns null if none available)
    AckEnvelope<String> envelope = queue.tryPoll();
    
    if (envelope != null) {
        try {
            // Process the message
            String message = envelope.payload();
            System.out.println("Processing: " + message);
            
            // Do your work here...
            processMessage(message);
            
            // Acknowledge successful processing
            envelope.acknowledge();
            
        } catch (Exception e) {
            // Don't acknowledge on error - message will be redelivered
            System.err.println("Failed to process message: " + e.getMessage());
        }
    }
}
```

### Blocking Poll

Wait for a message if the queue is empty:

```java
// This blocks until a message is available
AckEnvelope<String> envelope = queue.poll();

try {
    processMessage(envelope.payload());
    envelope.acknowledge();
} catch (Exception e) {
    // Message will be redelivered
    e.printStackTrace();
}
```

### Custom Message Types

Use your own serialization for complex types:

```java
import com.fasterxml.jackson.databind.ObjectMapper;

public class PaymentRequest {
    public String orderId;
    public double amount;
    public String currency;
    
    // Constructors, getters, setters...
}

// Create a custom serializer
MessageSerializer<PaymentRequest> serializer = new MessageSerializer<PaymentRequest>() {
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public String getTypeName() {
        return PaymentRequest.class.getName();
    }
    
    @Override
    public byte[] serialize(PaymentRequest payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
    
    @Override
    public PaymentRequest deserialize(byte[] serialized) {
        try {
            return objectMapper.readValue(serialized, PaymentRequest.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Deserialization failed", e);
        }
    }
};

// Use it
try (DelayedQueue<PaymentRequest> queue = DelayedQueueJDBC.create(
    serializer,
    queueConfig
)) {
    PaymentRequest payment = new PaymentRequest();
    payment.orderId = "ORD-123";
    payment.amount = 99.99;
    payment.currency = "USD";
    
    queue.offerOrUpdate(
        "payment-" + payment.orderId,
        payment,
        Instant.now().plus(Duration.ofMinutes(5))
    );
}
```

## Real-World Scenarios

### Scenario 1: HTTP Server with Business Hours

**Use Case**: Your API receives payment transactions 24/7, but your payment processor only operates during business hours (08:00 - 20:00). Transactions received outside business hours should be automatically scheduled for processing at 08:00 the next morning.

```java
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.funfix.delayedqueue.jvm.*;

import java.time.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PaymentProcessingServer {
    
    private static final LocalTime BUSINESS_HOURS_START = LocalTime.of(8, 0);
    private static final LocalTime BUSINESS_HOURS_END = LocalTime.of(20, 0);
    private static final ZoneId BUSINESS_TIMEZONE = ZoneId.of("America/New_York");
    
    private final DelayedQueue<PaymentTransaction> paymentQueue;
    private final ScheduledExecutorService workerPool;
    
    public PaymentProcessingServer() throws Exception {
        // Initialize the queue
        JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
            "jdbc:sqlite:/var/app/payments.db",
            JdbcDriver.Sqlite,
            null, null, null
        );
        
        DelayedQueueJDBCConfig queueConfig = DelayedQueueJDBCConfig.create(
            dbConfig,
            "payment_queue",
            "payments"
        );
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        this.paymentQueue = DelayedQueueJDBC.create(
            new PaymentTransactionSerializer(),
            queueConfig
        );
        
        // Start background workers
        this.workerPool = Executors.newScheduledThreadPool(4);
        startWorkers();
    }
    
    public void start() {
        Javalin app = Javalin.create().start(8080);
        
        app.post("/api/payments", this::handlePayment);
        
        System.out.println("Payment server started on http://localhost:8080");
    }
    
    private void handlePayment(Context ctx) {
        try {
            PaymentTransaction transaction = ctx.bodyAsClass(PaymentTransaction.class);
            
            // Calculate when to process this payment
            Instant scheduleAt = calculateProcessingTime(Instant.now());
            
            // Schedule the payment
            OfferOutcome outcome = paymentQueue.offerOrUpdate(
                "payment-" + transaction.transactionId,
                transaction,
                scheduleAt
            );
            
            ctx.json(new Response(
                "Payment scheduled",
                transaction.transactionId,
                scheduleAt
            ));
            
        } catch (Exception e) {
            ctx.status(500).json(new ErrorResponse(e.getMessage()));
        }
    }
    
    /**
     * Calculates when a payment should be processed based on business hours.
     * 
     * - If current time is during business hours (08:00-20:00): process immediately
     * - If outside business hours: schedule for 08:00 the next business day
     */
    private Instant calculateProcessingTime(Instant now) {
        ZonedDateTime zonedNow = now.atZone(BUSINESS_TIMEZONE);
        LocalTime currentTime = zonedNow.toLocalTime();
        
        if (isBusinessHours(currentTime)) {
            // Process immediately
            return now;
        } else {
            // Schedule for next opening at 08:00
            ZonedDateTime nextOpening;
            
            if (currentTime.isBefore(BUSINESS_HOURS_START)) {
                // Before 08:00 - schedule for today at 08:00
                nextOpening = zonedNow.with(BUSINESS_HOURS_START);
            } else {
                // After 20:00 - schedule for tomorrow at 08:00
                nextOpening = zonedNow.plusDays(1).with(BUSINESS_HOURS_START);
            }
            
            return nextOpening.toInstant();
        }
    }
    
    private boolean isBusinessHours(LocalTime time) {
        return !time.isBefore(BUSINESS_HOURS_START) && time.isBefore(BUSINESS_HOURS_END);
    }
    
    /**
     * Background workers continuously poll for payments and process them.
     */
    private void startWorkers() {
        // Start 4 worker threads that poll every 500ms
        for (int i = 0; i < 4; i++) {
            int workerId = i + 1;
            workerPool.scheduleWithFixedDelay(
                () -> processPayments(workerId),
                0,
                500,
                TimeUnit.MILLISECONDS
            );
        }
        
        System.out.println("Started 4 payment processing workers");
    }
    
    private void processPayments(int workerId) {
        try {
            AckEnvelope<PaymentTransaction> envelope = paymentQueue.tryPoll();
            
            if (envelope != null) {
                PaymentTransaction payment = envelope.payload();
                
                System.out.printf(
                    "[Worker %d] Processing payment %s for $%.2f%n",
                    workerId,
                    payment.transactionId,
                    payment.amount
                );
                
                try {
                    // Process the payment (call external payment processor)
                    processPaymentWithProcessor(payment);
                    
                    // Mark as successfully processed
                    envelope.acknowledge();
                    
                    System.out.printf(
                        "[Worker %d] Payment %s completed successfully%n",
                        workerId,
                        payment.transactionId
                    );
                    
                } catch (Exception e) {
                    System.err.printf(
                        "[Worker %d] Payment %s failed: %s (will retry)%n",
                        workerId,
                        payment.transactionId,
                        e.getMessage()
                    );
                    // Don't acknowledge - will be redelivered
                }
            }
            
        } catch (Exception e) {
            System.err.println("Worker error: " + e.getMessage());
        }
    }
    
    private void processPaymentWithProcessor(PaymentTransaction payment) throws Exception {
        // Simulate payment processing
        Thread.sleep(100);
        
        // In reality, you'd call your payment processor API here
        // paymentProcessorClient.charge(payment.cardToken, payment.amount);
    }
    
    public void shutdown() {
        System.out.println("Shutting down payment server...");
        workerPool.shutdown();
        try {
            paymentQueue.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Data classes
    public static class PaymentTransaction {
        public String transactionId;
        public String customerId;
        public double amount;
        public String currency;
        public String cardToken;
    }
    
    public static class PaymentTransactionSerializer implements MessageSerializer<PaymentTransaction> {
        private final com.fasterxml.jackson.databind.ObjectMapper mapper = 
            new com.fasterxml.jackson.databind.ObjectMapper();
        
        @Override
        public String getTypeName() {
            return PaymentTransaction.class.getName();
        }
        
        @Override
        public byte[] serialize(PaymentTransaction payload) {
            try {
                return mapper.writeValueAsBytes(payload);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public PaymentTransaction deserialize(byte[] serialized) {
            try {
                return mapper.readValue(serialized, PaymentTransaction.class);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
    
    public static class Response {
        public String status;
        public String transactionId;
        public Instant scheduledFor;
        
        public Response(String status, String transactionId, Instant scheduledFor) {
            this.status = status;
            this.transactionId = transactionId;
            this.scheduledFor = scheduledFor;
        }
    }
    
    public static class ErrorResponse {
        public String error;
        
        public ErrorResponse(String error) {
            this.error = error;
        }
    }
    
    public static void main(String[] args) throws Exception {
        PaymentProcessingServer server = new PaymentProcessingServer();
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        
        server.start();
    }
}
```

**Key Points:**
- **Business hours logic**: Automatically delays processing to 08:00 if received outside hours
- **Worker pool**: Multiple threads continuously poll the queue for work
- **Graceful shutdown**: Properly closes resources using try-with-resources and shutdown hooks
- **Automatic retry**: Failed payments are retried because `acknowledge()` isn't called

### Scenario 2: Daily Cron Job with Multi-Node Processing

**Use Case**: You need to run a daily report generation job at 02:00 AM. Multiple application instances are running (for high availability), but the job should only run once per day - whichever node picks it up first wins the race.

```java
import org.funfix.delayedqueue.jvm.*;

import java.time.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DailyReportGenerator {
    
    private static final ZoneId SCHEDULE_TIMEZONE = ZoneId.of("America/New_York");
    
    private final DelayedQueue<ReportTask> reportQueue;
    private final ScheduledExecutorService executor;
    private AutoCloseable cronSchedule;
    
    public DailyReportGenerator() throws Exception {
        // Setup the queue
        JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
            "jdbc:postgresql://db.example.com:5432/myapp",
            JdbcDriver.PostgreSQL,
            "appuser",
            "password",
            new JdbcDatabasePoolConfig(
                10,  // maxPoolSize
                30000  // connectionTimeoutMs
            )
        );
        
        DelayedQueueJDBCConfig queueConfig = DelayedQueueJDBCConfig.create(
            dbConfig,
            "cron_tasks",
            "daily-reports"
        );
        
        DelayedQueueJDBC.runMigrations(queueConfig);
        
        this.reportQueue = DelayedQueueJDBC.create(
            new ReportTaskSerializer(),
            queueConfig
        );
        
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     * Starts the cron schedule and worker.
     * 
     * The cron service automatically:
     * - Schedules tasks for 02:00 AM every day
     * - Handles configuration changes
     * - Cleans up old scheduled tasks
     * 
     * Multiple nodes can call this safely - only one will process each task.
     */
    public void start() throws Exception {
        // Define when reports should run (02:00 AM daily)
        CronDailySchedule schedule = CronDailySchedule.create(
            SCHEDULE_TIMEZONE,
            List.of(LocalTime.of(2, 0)),      // Run at 02:00
            Duration.ofDays(7),                // Schedule 7 days in advance
            Duration.ofHours(1)                // Check/update schedule every hour
        );
        
        // Install the daily schedule
        this.cronSchedule = reportQueue.getCron().installDailySchedule(
            "daily-report",
            schedule,
            scheduleTime -> new CronMessage<>(
                new ReportTask("daily-sales-report", scheduleTime),
                scheduleTime
            )
        );
        
        // Start the worker that processes the tasks
        executor.scheduleWithFixedDelay(
            this::processReports,
            0,
            30,  // Poll every 30 seconds
            TimeUnit.SECONDS
        );
        
        System.out.println("Daily report cron started");
    }
    
    /**
     * Worker that polls for and processes report tasks.
     * 
     * This runs on ALL nodes, but DelayedQueue guarantees that only
     * one node will successfully acquire any given task.
     */
    private void processReports() {
        try {
            AckEnvelope<ReportTask> envelope = reportQueue.tryPoll();
            
            if (envelope != null) {
                ReportTask task = envelope.payload();
                
                String nodeId = getNodeId();
                System.out.printf(
                    "[Node %s] Acquired report task: %s (scheduled for %s)%n",
                    nodeId,
                    task.reportType,
                    task.scheduledFor
                );
                
                try {
                    // Generate the report
                    generateReport(task);
                    
                    // Acknowledge - prevents other nodes from processing it
                    envelope.acknowledge();
                    
                    System.out.printf(
                        "[Node %s] Report completed: %s%n",
                        nodeId,
                        task.reportType
                    );
                    
                } catch (Exception e) {
                    System.err.printf(
                        "[Node %s] Report failed: %s - %s%n",
                        nodeId,
                        task.reportType,
                        e.getMessage()
                    );
                    // Don't acknowledge - another node can retry
                }
            }
            
        } catch (Exception e) {
            System.err.println("Worker error: " + e.getMessage());
        }
    }
    
    private void generateReport(ReportTask task) throws Exception {
        System.out.println("Generating " + task.reportType + "...");
        
        // Simulate report generation
        Thread.sleep(5000);
        
        // In reality:
        // - Query database for report data
        // - Generate PDF/Excel
        // - Upload to S3
        // - Send email notification
    }
    
    private String getNodeId() {
        // In a real application, this would be your actual node ID
        // Could be hostname, container ID, etc.
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    public void shutdown() throws Exception {
        System.out.println("Shutting down daily report generator...");
        
        if (cronSchedule != null) {
            cronSchedule.close();
        }
        
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        reportQueue.close();
    }
    
    // Data classes
    public static class ReportTask {
        public String reportType;
        public Instant scheduledFor;
        
        public ReportTask() {}
        
        public ReportTask(String reportType, Instant scheduledFor) {
            this.reportType = reportType;
            this.scheduledFor = scheduledFor;
        }
    }
    
    public static class ReportTaskSerializer implements MessageSerializer<ReportTask> {
        private final com.fasterxml.jackson.databind.ObjectMapper mapper = 
            new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        
        @Override
        public String getTypeName() {
            return ReportTask.class.getName();
        }
        
        @Override
        public byte[] serialize(ReportTask payload) {
            try {
                return mapper.writeValueAsBytes(payload);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public ReportTask deserialize(byte[] serialized) {
            try {
                return mapper.readValue(serialized, ReportTask.class);
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        DailyReportGenerator generator = new DailyReportGenerator();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                generator.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        
        generator.start();
        
        System.out.println("Daily report generator is running. Press Ctrl+C to stop.");
        
        // Keep the application running
        Thread.currentThread().join();
    }
}
```

**Key Points:**
- **Multi-node safe**: Only one node acquires each task through database-level locking
- **Automatic scheduling**: CronService handles creating future tasks
- **High availability**: If one node fails, another node picks up the work
- **Configuration-driven**: Schedule is defined declaratively
- **Graceful shutdown**: Properly releases all resources

**How the Race Works:**

1. **CronService** creates scheduled tasks in the database (e.g., one for each day at 02:00 AM)
2. **All nodes** run workers that call `tryPoll()`
3. **Database locking** ensures only one node successfully acquires each task
4. The **winning node** processes the task and calls `acknowledge()`
5. **Other nodes** get `null` from `tryPoll()` (task already taken)
6. If the winning node **fails** without acknowledging, the task becomes available again after the timeout

## Best Practices

### 1. Always Use Try-With-Resources

DelayedQueue implements `AutoCloseable` and manages database connections. Always ensure proper cleanup:

```java
// ✅ Good
try (DelayedQueue<String> queue = DelayedQueueJDBC.create(...)) {
    // Use queue
}

// ❌ Bad
DelayedQueue<String> queue = DelayedQueueJDBC.create(...);
// Forgot to close - connection leak!
```

### 2. Run Migrations Once

Don't run migrations on every queue creation - do it once during application startup:

```java
// ✅ Good - Run migrations at startup
public class Application {
    public static void main(String[] args) {
        DelayedQueueJDBCConfig config = createConfig();
        DelayedQueueJDBC.runMigrations(config);  // Once
        
        startApplication(config);
    }
}

// ❌ Bad - Running migrations every time
public DelayedQueue<String> getQueue() {
    DelayedQueueJDBCConfig config = createConfig();
    DelayedQueueJDBC.runMigrations(config);  // Don't do this repeatedly!
    return DelayedQueueJDBC.create(...);
}
```

### 3. Handle Acknowledgement Carefully

Only acknowledge messages after successful processing:

```java
AckEnvelope<String> envelope = queue.poll();

try {
    processMessage(envelope.payload());
    sendNotification(envelope.payload());
    updateDatabase(envelope.payload());
    
    // ✅ Only acknowledge after everything succeeds
    envelope.acknowledge();
    
} catch (Exception e) {
    // ✅ Don't acknowledge on failure - message will be redelivered
    logger.error("Processing failed, will retry", e);
}
```

### 4. Use Unique, Meaningful Keys

Keys should be:
- **Unique**: Identify the message/transaction uniquely
- **Meaningful**: Help with debugging and manual inspection
- **Stable**: Don't change between offer and acknowledgement

```java
// ✅ Good keys
queue.offerOrUpdate("payment-" + orderId, payment, deliveryTime);
queue.offerOrUpdate("email-" + userId + "-" + timestamp, email, deliveryTime);

// ❌ Bad keys (not unique or meaningful)
queue.offerOrUpdate(UUID.randomUUID().toString(), payment, deliveryTime);
queue.offerOrUpdate("msg", email, deliveryTime);
```

### 5. Configure Appropriate Timeouts

Adjust timeouts based on your processing time:

```java
// Default: 30 second acquire timeout, 100ms poll interval
DelayedQueueTimeConfig defaultConfig = DelayedQueueTimeConfig.DEFAULT;

// Custom: 5 minute timeout for long-running tasks
DelayedQueueTimeConfig customConfig = DelayedQueueTimeConfig.create(
    Duration.ofMinutes(5),    // acquireTimeout
    Duration.ofSeconds(1)     // pollInterval
);

DelayedQueueJDBCConfig config = new DelayedQueueJDBCConfig(
    dbConfig,
    "my_queue",
    customConfig,  // Use custom timeouts
    "my-queue-name"
);
```

### 6. Monitor Queue Depth

In production, monitor how many messages are in the queue:

```java
// Check if a specific message exists
boolean exists = queue.containsMessage("payment-12345");

// For monitoring, you can query the database directly
// (assuming table name is "delayed_queue")
String sql = "SELECT COUNT(*) FROM delayed_queue WHERE partition_kind = ?";
```

### 7. Use Connection Pooling for Production

For production with multiple workers, configure connection pooling:

```java
JdbcDatabasePoolConfig poolConfig = new JdbcDatabasePoolConfig(
    20,     // maxPoolSize - adjust based on your worker count
    30000   // connectionTimeoutMs
);

JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
    jdbcUrl,
    driver,
    username,
    password,
    poolConfig  // Enable pooling
);
```

### 8. Separate Queues by Concern

Use different queue names for different types of work:

```java
// Different queues for different concerns
DelayedQueue<Email> emailQueue = DelayedQueueJDBC.create(
    emailSerializer,
    DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue", "emails")
);

DelayedQueue<Payment> paymentQueue = DelayedQueueJDBC.create(
    paymentSerializer,
    DelayedQueueJDBCConfig.create(dbConfig, "delayed_queue", "payments")
);

// They can share the same table, but are isolated by queue name
```

### 9. Implement Proper Error Handling

```java
private void processMessage(AckEnvelope<Task> envelope) {
    try {
        Task task = envelope.payload();
        
        // Process the task
        Result result = businessLogic.process(task);
        
        // Save the result
        database.save(result);
        
        // Only acknowledge after all side effects are committed
        envelope.acknowledge();
        
    } catch (TransientException e) {
        // Transient errors (network issues, etc.) - don't acknowledge
        // The message will be retried
        logger.warn("Transient error, will retry: {}", e.getMessage());
        
    } catch (PermanentException e) {
        // Permanent errors (invalid data, etc.) - acknowledge to prevent infinite retries
        logger.error("Permanent error, discarding message: {}", e.getMessage());
        envelope.acknowledge();
        
        // Optionally save to dead letter queue
        deadLetterQueue.save(envelope.payload(), e);
    }
}
```

### 10. Test with Time

Use a custom `Clock` for testing time-dependent behavior:

```java
// In tests
import java.time.Clock;

Clock fixedClock = Clock.fixed(
    Instant.parse("2024-01-01T12:00:00Z"),
    ZoneId.of("UTC")
);

DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig,
    fixedClock  // Inject clock for testing
);

// Schedule for "future"
queue.offerOrUpdate("test", "message", fixedClock.instant().plusSeconds(60));

// Message not available yet
assertNull(queue.tryPoll());
```

---

## Additional Resources

- [Javadoc](https://javadoc.io/doc/org.funfix/delayedqueue-jvm)
- [Internals Documentation](./internals.md)
- [GitHub Repository](https://github.com/funfix/database)

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/funfix/database).
