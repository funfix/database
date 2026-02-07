# DelayedQueue: Java Developer Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Basic Usage](#basic-usage)
4. [Real-World Scenarios](#real-world-scenarios)
  - [Scenario 1: Scheduling Outside Business Hours](#scenario-1-scheduling-outside-business-hours)
  - [Scenario 2: Daily Cron Job with Multi-Node Coordination](#scenario-2-daily-cron-job-with-multi-node-coordination)
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
  <version>0.3.0</version>
</dependency>
```

**Gradle:**
```kotlin
dependencies {
    implementation("org.funfix:delayedqueue-jvm:0.3.0")
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

public class Task {
    public String taskId;
    public String description;
    
    // Constructors, getters, setters...
}

// Create a custom serializer
MessageSerializer<Task> serializer = new MessageSerializer<Task>() {
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public String getTypeName() {
        return Task.class.getName();
    }
    
    @Override
    public byte[] serialize(Task payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
    
    @Override
    public Task deserialize(byte[] serialized) {
        try {
            return objectMapper.readValue(serialized, Task.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Deserialization failed", e);
        }
    }
};

// Use it
try (DelayedQueue<Task> queue = DelayedQueueJDBC.create(
    serializer,
    queueConfig
)) {
    Task task = new Task();
    task.taskId = "TASK-123";
    task.description = "Process monthly report";
    
    queue.offerOrUpdate(
        "task-" + task.taskId,
        task,
        Instant.now().plus(Duration.ofMinutes(5))
    );
}
```

### Cron-like Scheduling

Schedule recurring tasks using the CronService:

#### Periodic Tick

Run a task every N hours/minutes:

```java
try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig
)) {
    // Schedule a message every hour
    AutoCloseable cronJob = queue.getCron().installPeriodicTick(
        "health-check",              // Key prefix
        Duration.ofHours(1),         // Run every hour
        instant -> "Health check at " + instant
    );
    
    // Later, when shutting down
    cronJob.close();
}
```

#### Daily Schedule

Run tasks at specific times each day:

```java
try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig
)) {
    // Run at 2:00 AM and 2:00 PM daily (Eastern time)
    CronDailySchedule schedule = CronDailySchedule.create(
        ZoneId.of("America/New_York"),
        List.of(LocalTime.of(2, 0), LocalTime.of(14, 0)),
        Duration.ofDays(7),          // Schedule 7 days ahead
        Duration.ofHours(1)          // Check every hour
    );
    
    AutoCloseable cronJob = queue.getCron().installDailySchedule(
        "daily-backup",
        schedule,
        instant -> new CronMessage<>("Run backup", instant)
    );
    
    // Later, when shutting down
    cronJob.close();
}
```

#### Install One-Time Schedule

For a fixed set of future events:

```java
try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
    MessageSerializer.forStrings(),
    queueConfig
)) {
    // Create a config hash to identify this set of messages
    CronConfigHash configHash = CronConfigHash.fromPeriodicTick(Duration.ofDays(1));
    
    // Schedule multiple messages at once
    List<CronMessage<String>> messages = List.of(
        new CronMessage<>("Event 1", Instant.now().plus(Duration.ofHours(1))),
        new CronMessage<>("Event 2", Instant.now().plus(Duration.ofHours(2))),
        new CronMessage<>("Event 3", Instant.now().plus(Duration.ofHours(3)))
    );
    
    queue.getCron().installTick(configHash, "event-batch-", messages);
    
    // Update or remove them later
    queue.getCron().uninstallTick(configHash, "event-batch-");
}
```

## Real-World Scenarios

### Scenario 1: Scheduling Outside Business Hours

**Use Case**: You need to send email notifications, but want to avoid sending them during nighttime hours (20:00 - 08:00). Messages queued during off-hours should be scheduled for 08:00 the next morning.

```java
import org.funfix.delayedqueue.jvm.*;
import java.time.*;

public class EmailScheduler {
    
    private static final LocalTime QUIET_HOURS_END = LocalTime.of(8, 0);
    private static final LocalTime QUIET_HOURS_START = LocalTime.of(20, 0);
    
    public static void main(String[] args) throws Exception {
        // Setup queue
        JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
            "jdbc:sqlite:/tmp/emails.db",
            JdbcDriver.Sqlite,
            null, null, null
        );
        
        DelayedQueueJDBCConfig config = DelayedQueueJDBCConfig.create(
            dbConfig, "email_queue", "emails"
        );
        DelayedQueueJDBC.runMigrations(config);
        
        try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
            MessageSerializer.forStrings(), config
        )) {
            // Schedule an email notification
            String emailMessage = "Order #12345 has shipped";
            Instant sendAt = calculateSendTime(Instant.now());
            
            queue.offerOrUpdate("email-order-12345", emailMessage, sendAt);
            System.out.println("Email scheduled for: " + sendAt);
            
            // Worker: poll and send emails
            AckEnvelope<String> envelope = queue.tryPoll();
            if (envelope != null) {
                try {
                    sendEmail(envelope.payload());
                    envelope.acknowledge();
                    System.out.println("Email sent successfully");
                } catch (Exception e) {
                    // Don't acknowledge - will retry later
                    System.err.println("Failed to send: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Calculate when to send based on quiet hours.
     * - During daytime (08:00-20:00): send immediately
     * - During nighttime: schedule for 08:00 next morning
     */
    static Instant calculateSendTime(Instant now) {
        ZonedDateTime zdt = now.atZone(ZoneId.systemDefault());
        LocalTime time = zdt.toLocalTime();
        
        if (time.isBefore(QUIET_HOURS_END)) {
            // Before 08:00 - send at 08:00 today
            return zdt.with(QUIET_HOURS_END).toInstant();
        } else if (time.isBefore(QUIET_HOURS_START)) {
            // 08:00-20:00 - send now
            return now;
        } else {
            // After 20:00 - send at 08:00 tomorrow
            return zdt.plusDays(1).with(QUIET_HOURS_END).toInstant();
        }
    }
    
    static void sendEmail(String message) {
        System.out.println("Sending email: " + message);
        // Actually send the email...
    }
}
```

**Key Points:**
- `calculateSendTime()` implements the business hours logic
- `offerOrUpdate()` schedules the message for the calculated time
- `tryPoll()` retrieves messages when they're ready
- `acknowledge()` marks successful processing (or skip it to retry)

### Scenario 2: Daily Cron Job with Multi-Node Coordination

**Use Case**: Run a daily data cleanup job at 02:00 AM. Multiple application instances are running for high availability, but the job should only run once per day - the first node to poll wins.

```java
import org.funfix.delayedqueue.jvm.*;
import java.time.*;
import java.util.List;

public class DailyCleanupJob {
    
    public static void main(String[] args) throws Exception {
        // Setup queue
        JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
            "jdbc:postgresql://db.example.com:5432/myapp",
            JdbcDriver.PostgreSQL,
            "appuser",
            "password",
            null
        );
        
        DelayedQueueJDBCConfig config = DelayedQueueJDBCConfig.create(
            dbConfig, "scheduled_jobs", "cleanup"
        );
        DelayedQueueJDBC.runMigrations(config);
        
        try (DelayedQueue<String> queue = DelayedQueueJDBC.create(
            MessageSerializer.forStrings(), config
        )) {
            // Install daily schedule: run at 02:00 AM Eastern time
            CronDailySchedule schedule = CronDailySchedule.create(
                ZoneId.of("America/New_York"),
                List.of(LocalTime.of(2, 0)),    // 02:00 AM
                Duration.ofDays(7),              // Schedule 7 days ahead
                Duration.ofHours(1)              // Update schedule hourly
            );
            
            AutoCloseable cronJob = queue.getCron().installDailySchedule(
                "daily-cleanup",
                schedule,
                instant -> new CronMessage<>("Cleanup job for " + instant, instant)
            );
            
            // Worker loop: continuously poll for jobs
            // This runs on ALL nodes, but only one will get each job
            while (true) {
                AckEnvelope<String> envelope = queue.tryPoll();
                
                if (envelope != null) {
                    System.out.println("[Node-" + getNodeId() + "] Got job: " + 
                        envelope.payload());
                    
                    try {
                        runCleanup();
                        envelope.acknowledge();  // Success - job done
                        System.out.println("Cleanup completed");
                    } catch (Exception e) {
                        // Don't acknowledge - another node can retry
                        System.err.println("Cleanup failed: " + e.getMessage());
                    }
                } else {
                    Thread.sleep(30_000);  // Wait 30s before polling again
                }
            }
        }
    }
    
    static void runCleanup() {
        System.out.println("Deleting old records...");
        // Delete expired sessions, old logs, etc.
    }
    
    static String getNodeId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
```

**Key Points:**
- `CronDailySchedule` automatically creates future tasks at 02:00 AM
- Multiple nodes can run this code - **database locking ensures only one gets each task**
- `tryPoll()` returns `null` on nodes that don't win the race
- If the winning node fails without calling `acknowledge()`, the task becomes available again

**How Multi-Node Works:**
1. CronService creates scheduled tasks in the database (one per day at 02:00 AM)
2. All nodes continuously call `tryPoll()`
3. Database-level locking ensures only one node acquires each task
4. The winning node processes and calls `acknowledge()`
5. Other nodes get `null` (task already taken)
6. If the winner crashes, the task is automatically retried after the timeout

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

### 4. Configure Appropriate Timeouts

Adjust timeouts based on your processing time:

```java
// Default used for RDBMS access
DelayedQueueTimeConfig defaultConfig = DelayedQueueTimeConfig.DEFAULT_JDBC;

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

### 5. Separate Queues by Concern

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

// They can share the same table, but are isolated by queue name + message type
```

### 6. Implement Proper Error Handling

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

### 7. Test with Mocked Time

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
