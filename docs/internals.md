# DelayedQueue and CronService Internals

This document explains the internal workings of the DelayedQueue and CronService implementations, focusing on PostgreSQL queries and concurrency guarantees.

## Table of Contents

1. [Database Schema](#database-schema)
2. [DelayedQueue Operations](#delayedqueue-operations)
   - [offer()](#offer)
   - [tryPoll()](#trypoll)
   - [offerBatch()](#offerbatch)
   - [tryPollMany()](#trypollmany)
3. [Concurrency Guarantees](#concurrency-guarantees)
4. [CronService Implementation](#cronservice-implementation)
5. [Why It Works and Is Reliable](#why-it-works-and-is-reliable)

## Database Schema

The DelayedQueue uses a single table with the following PostgreSQL schema:

```sql
CREATE TABLE "delayed_queue" (
    "id" BIGSERIAL PRIMARY KEY,
    "pKey" VARCHAR(200) NOT NULL,
    "pKind" VARCHAR(100) NOT NULL,
    "payload" BYTEA NOT NULL,
    "scheduledAt" BIGINT NOT NULL,
    "scheduledAtInitially" BIGINT NOT NULL,
    "lockUuid" VARCHAR(36) NULL,
    "createdAt" BIGINT NOT NULL
);

CREATE UNIQUE INDEX "delayed_queue__PKeyPlusKindUniqueIndex" 
ON "delayed_queue"("pKey", "pKind");

CREATE INDEX "delayed_queue__KindPlusScheduledAtIndex" 
ON "delayed_queue"("pKind", "scheduledAt");

CREATE INDEX "delayed_queue__LockUuidPlusIdIndex" 
ON "delayed_queue"("lockUuid", "id");
```

### Schema Details

- **id**: Auto-incrementing primary key
- **pKey**: User-provided unique message key
- **pKind**: Partition kind (computed from queue name + serializer type)
- **payload**: Binary message payload
- **scheduledAt**: When the message should be delivered (epoch milliseconds)
- **scheduledAtInitially**: Original scheduled time (used to detect redelivery)
- **lockUuid**: Temporary lock ID when a message is being processed
- **createdAt**: When the message was created (epoch milliseconds)

### Indexes

1. **Unique index on (pKey, pKind)**: Ensures message uniqueness within a queue
2. **Index on (pKind, scheduledAt)**: Optimizes polling by scheduled time
3. **Index on (lockUuid, id)**: Optimizes acknowledgment and batch retrieval

## DelayedQueue Operations

### offer()

The `offer()` operation inserts or updates a single message. It uses a two-phase approach:

#### Phase 1: Optimistic INSERT

```sql
INSERT INTO "delayed_queue"
(
    "pKey", 
    "pKind", 
    "payload", 
    "scheduledAt", 
    "scheduledAtInitially", 
    "createdAt"
)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT ("pKey", "pKind") DO NOTHING
```

**Query Parameters:**
- pKey: Message key
- pKind: Queue partition
- payload: Serialized message
- scheduledAt: Delivery time
- scheduledAtInitially: Same as scheduledAt
- createdAt: Current timestamp

**Result:**
- Returns 1 if inserted successfully → `OfferOutcome.Created`
- Returns 0 if key already exists → Proceed to Phase 2 (if canUpdate=true)

#### Phase 2: SELECT FOR UPDATE + Conditional UPDATE (if canUpdate=true)

If the INSERT fails and `canUpdate` is true, the operation retries with a transactional update:

**SELECT FOR UPDATE:**
```sql
SELECT 
    "id", 
    "pKey", 
    "pKind", 
    "payload", 
    "scheduledAt", 
    "scheduledAtInitially", 
    "lockUuid", 
    "createdAt"
FROM "delayed_queue"
WHERE "pKey" = ? AND "pKind" = ?
LIMIT 1
FOR UPDATE
```

**Guarded UPDATE (Compare-And-Swap):**
```sql
UPDATE "delayed_queue"
SET 
    "payload" = ?,
    "scheduledAt" = ?,
    "scheduledAtInitially" = ?,
    "lockUuid" = ?,
    "createdAt" = ?
WHERE 
    "pKey" = ?
    AND "pKind" = ?
    AND "scheduledAtInitially" = ?
    AND "createdAt" = ?
```

**Why the WHERE clause checks scheduledAtInitially and createdAt:**
This is a **Compare-And-Swap (CAS)** operation. It only updates if the row hasn't been modified by another transaction. If the WHERE clause doesn't match (someone else updated it), the UPDATE returns 0 rows affected, and the operation retries.

**Retry Loop:**
The operation continues retrying the SELECT FOR UPDATE + UPDATE until either:
- The update succeeds → `OfferOutcome.Updated`
- The row is a duplicate (same payload/schedule) → `OfferOutcome.Ignored`
- The row disappears (deleted by consumer) → Retry from the beginning

### tryPoll()

The `tryPoll()` operation retrieves and locks a single message ready for processing:

#### Step 1: SELECT with FOR UPDATE SKIP LOCKED

```sql
SELECT 
    "id", 
    "pKey", 
    "pKind", 
    "payload", 
    "scheduledAt", 
    "scheduledAtInitially", 
    "lockUuid", 
    "createdAt"
FROM "delayed_queue"
WHERE "pKind" = ? AND "scheduledAt" <= ?
ORDER BY "scheduledAt"
LIMIT 1
FOR UPDATE SKIP LOCKED
```

**Query Parameters:**
- pKind: Queue partition
- scheduledAt: Current timestamp (only messages scheduled for now or earlier)

**FOR UPDATE SKIP LOCKED behavior:**
- Locks the row for update
- **SKIP LOCKED**: If another transaction already locked the row, skip it and try the next one
- This is crucial for multi-consumer concurrency: each consumer gets a different message

**Result:**
- If no rows found → Return null (no messages available)
- If row found → Proceed to Step 2

#### Step 2: Acquire the message via UPDATE

```sql
UPDATE "delayed_queue"
SET "scheduledAt" = ?,
    "lockUuid" = ?
WHERE "pKey" = ?
  AND "pKind" = ?
  AND "scheduledAt" = ?
```

**Query Parameters:**
- scheduledAt (SET): now + acquireTimeout (e.g., now + 5 minutes)
- lockUuid (SET): Random UUID for this acquisition
- pKey (WHERE): Message key
- pKind (WHERE): Queue partition
- scheduledAt (WHERE): Original scheduledAt from Step 1

**Why check scheduledAt in WHERE:**
This is optimistic locking. If another transaction updated the row between SELECT and UPDATE, the scheduledAt will be different, and the UPDATE will affect 0 rows. The operation then retries from Step 1.

**Result:**
- If UPDATE affects 1 row → Message acquired successfully
- If UPDATE affects 0 rows → Someone else acquired it, retry from Step 1

#### Step 3: Return message envelope with acknowledge function

The acknowledge function deletes the message by lockUuid:

```sql
DELETE FROM "delayed_queue" 
WHERE "lockUuid" = ?
```

**Timeout behavior:**
If the consumer doesn't acknowledge within `acquireTimeout`, the message's `scheduledAt` (set to now + timeout) will expire, making it available for redelivery.

### offerBatch()

The `offerBatch()` operation inserts multiple messages efficiently:

#### Step 1: Check for existing keys

```sql
SELECT "pKey" 
FROM "delayed_queue" 
WHERE 
    "pKind" = ? 
    AND "pKey" IN (?, ?, ?, ...)
```

**Why this query:**
Avoids the N+1 problem. Instead of checking each key individually, we check all keys in a single query.

**Result:**
Returns the set of keys that already exist in the database.

#### Step 2: Batch INSERT for non-existing keys

```sql
INSERT INTO "delayed_queue"
(
    "pKey", 
    "pKind", 
    "payload", 
    "scheduledAt", 
    "scheduledAtInitially", 
    "lockUuid", 
    "createdAt"
)
VALUES
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    ...
```

**Batch size:**
The implementation chunks insertions into batches of up to 200 rows to avoid exceeding parameter limits.

**Error handling:**
If a duplicate key error occurs (race condition: another transaction inserted between Step 1 and Step 2), the entire batch falls back to one-by-one `offer()` calls.

#### Step 3: Fallback to one-by-one for updates

Messages that:
- Already existed (from Step 1)
- Need to be updated (`canUpdate=true`)
- Failed to insert (race condition)

Are processed one-by-one using the standard `offer()` operation.

### tryPollMany()

The `tryPollMany()` operation retrieves and locks multiple messages in a single transaction:

#### Single Optimistic UPDATE with Subquery

```sql
UPDATE "delayed_queue"
SET 
    "lockUuid" = ?,
    "scheduledAt" = ?
WHERE "id" IN (
    SELECT "id"
    FROM "delayed_queue"
    WHERE 
        "pKind" = ? AND "scheduledAt" <= ?
    ORDER BY "scheduledAt"
    LIMIT ?
    FOR UPDATE SKIP LOCKED
)
```

**Query Parameters:**
- lockUuid (SET): Random UUID for this batch
- scheduledAt (SET): now + acquireTimeout
- pKind (WHERE subquery): Queue partition
- scheduledAt (WHERE subquery): Current timestamp
- LIMIT (subquery): Batch size (e.g., 10)

**How it works:**
1. The subquery selects up to N message IDs ready for delivery
2. FOR UPDATE SKIP LOCKED ensures each consumer gets different messages
3. The outer UPDATE atomically locks all selected messages

**Result:**
Returns the number of rows updated (acquired).

#### Retrieve locked messages

```sql
SELECT 
    "id",
    "pKey", 
    "pKind", 
    "payload", 
    "scheduledAt", 
    "scheduledAtInitially", 
    "lockUuid", 
    "createdAt"
FROM "delayed_queue"
WHERE "lockUuid" = ?
ORDER BY "id"
LIMIT ?
```

**Why ORDER BY "id" and pagination:**
For large batches, we retrieve messages in chunks (100 at a time) to avoid loading too many rows into memory at once.

**Acknowledge function:**
Same as `tryPoll()` - deletes all messages with the lockUuid:

```sql
DELETE FROM "delayed_queue" 
WHERE "lockUuid" = ?
```

## Concurrency Guarantees

The DelayedQueue implementation provides strong concurrency guarantees for many producers and many consumers:

### Multi-Producer Safety

**Challenge:** Multiple producers trying to insert the same key simultaneously.

**Solution:**

1. **Unique constraint on (pKey, pKind)**: PostgreSQL enforces uniqueness at the database level
2. **ON CONFLICT DO NOTHING**: First transaction to commit wins; others get 0 rows affected
3. **Retry with SELECT FOR UPDATE**: For updates, row-level locks prevent lost updates
4. **Compare-And-Swap updates**: The WHERE clause checks `scheduledAtInitially` and `createdAt` to detect concurrent modifications

**Example scenario:**
```
T1: INSERT key="msg1" → Success
T2: INSERT key="msg1" → ON CONFLICT, returns 0 rows
T2: SELECT FOR UPDATE key="msg1" → Waits for T1 to commit
T1: COMMIT
T2: Gets lock, reads current row, performs CAS UPDATE
```

### Multi-Consumer Safety

**Challenge:** Multiple consumers trying to poll the same messages simultaneously.

**Solution: FOR UPDATE SKIP LOCKED**

This PostgreSQL feature is critical for multi-consumer scenarios:

```sql
SELECT ... FROM delayed_queue
WHERE pKind = ? AND scheduledAt <= ?
ORDER BY scheduledAt
LIMIT 1
FOR UPDATE SKIP LOCKED
```

**Behavior:**
- **FOR UPDATE**: Locks the row(s) for update
- **SKIP LOCKED**: If a row is already locked by another transaction, skip it and try the next row

**Example with 3 consumers and 3 messages:**

```
Time T1: Database has messages [M1, M2, M3]

Consumer A: SELECT ... FOR UPDATE SKIP LOCKED → Locks M1
Consumer B: SELECT ... FOR UPDATE SKIP LOCKED → M1 is locked, skips to M2, locks M2
Consumer C: SELECT ... FOR UPDATE SKIP LOCKED → M1 and M2 locked, skips to M3, locks M3

All consumers proceed concurrently with different messages.
```

**Without SKIP LOCKED:**
Consumers would block waiting for locked rows, creating a serial bottleneck.

### Optimistic Locking for Acquisition

After SELECT FOR UPDATE SKIP LOCKED, the UPDATE checks the original `scheduledAt`:

```sql
UPDATE delayed_queue
SET scheduledAt = ?, lockUuid = ?
WHERE pKey = ? AND pKind = ? AND scheduledAt = ?
```

**Why:** Between SELECT and UPDATE, another transaction could have:
- Deleted the row (consumer acknowledged it)
- Updated the row (timeout expired, message redelivered)

If the UPDATE affects 0 rows, the consumer retries from the beginning.

### Message Timeout and Redelivery

**Scenario:** Consumer crashes after polling but before acknowledging.

**Mechanism:**
1. Consumer polls message, UPDATE sets `scheduledAt = now + acquireTimeout` (e.g., +5 minutes)
2. Consumer crashes, never acknowledges
3. After 5 minutes, the message's `scheduledAt` is in the past again
4. Another consumer can poll it (redelivery)

**Detection:**
`scheduledAtInitially` remains unchanged. If `scheduledAt > scheduledAtInitially`, it's a redelivery.

### Transaction Isolation

All operations use **READ COMMITTED** isolation level (PostgreSQL default):
- Each statement sees a consistent snapshot of committed data
- Row-level locks prevent concurrent modifications
- SELECT FOR UPDATE holds locks until transaction commits/rollbacks

## CronService Implementation

The CronService schedules recurring messages using the DelayedQueue as storage.

### How CronService Works

#### Key Format

Cron messages use a special key format:
```
{keyPrefix}/{configHash}/{timestamp}
```

Example: `daily-report/a3f5b9c7/1707321600000`

**Components:**
- **keyPrefix**: User-defined identifier (e.g., "daily-report")
- **configHash**: Hash of the cron configuration (schedule, period, etc.)
- **timestamp**: Scheduled execution time

**Why include configHash:**
When the configuration changes, old scheduled messages become obsolete. The configHash allows selective deletion of old messages while preserving current ones.

### installTick()

Installs a batch of cron messages for a specific tick.

#### Step 1: Delete old configuration messages

```sql
DELETE FROM "delayed_queue"
WHERE "pKind" = ?
  AND "pKey" LIKE ?
  AND "pKey" NOT LIKE ?
```

**Query Parameters:**
- pKind: Queue partition
- LIKE pattern 1: `{keyPrefix}/%` (all messages with this prefix)
- NOT LIKE pattern: `{keyPrefix}/{configHash}%` (except current config)

**Example:**
- keyPrefix = "daily-report"
- configHash = "a3f5b9c7"

```sql
DELETE FROM "delayed_queue"
WHERE "pKind" = 'my-queue|String'
  AND "pKey" LIKE 'daily-report/%'
  AND "pKey" NOT LIKE 'daily-report/a3f5b9c7%'
```

**Result:**
Deletes all cron messages for "daily-report" except those with the current configHash.

#### Step 2: Batch insert/update new messages

Uses `offerBatch()` to insert all scheduled cron messages. On the first run, `canUpdate=true` to reschedule existing messages. On subsequent runs, `canUpdate=false` to avoid duplicates.

### uninstallTick()

Removes all cron messages for a specific configuration:

```sql
DELETE FROM "delayed_queue" 
WHERE 
    "pKind" = ? 
    AND "pKey" LIKE ?
```

**Query Parameters:**
- pKind: Queue partition
- LIKE pattern: `{keyPrefix}/{configHash}%`

**Example:**
```sql
DELETE FROM "delayed_queue"
WHERE "pKind" = 'my-queue|String'
  AND "pKey" LIKE 'daily-report/a3f5b9c7%'
```

### install()

Creates a background scheduler that periodically calls `installTick()`.

**Scheduling:**
- Uses a single-thread ScheduledExecutorService
- Executes with fixed delay (scheduleInterval)
- First execution: `canUpdate=true` (reschedule existing)
- Subsequent executions: `canUpdate=false` (avoid duplicates)

**Example workflow for periodic ticks:**

```
Period: 1 hour
Schedule interval: 15 minutes (period / 4)

T0:    installTick() with messages [T0+1h, T0+2h, T0+3h, T0+4h]
T0+15: installTick() with messages [T0+1h15m, T0+2h15m, T0+3h15m, T0+4h15m]
       (Deletes old config messages, inserts new ones)
T0+30: installTick() with messages [T0+1h30m, T0+2h30m, T0+3h30m, T0+4h30m]
...
```

### Daily Schedule

For daily schedules (e.g., "run at 2 AM every day"), the `installDailySchedule()` method:

1. Calculates next execution times based on the schedule
2. Generates messages for each execution
3. Uses the same `installTick()` mechanism

**Example:**
```
Schedule: Run at 02:00:00 UTC daily
Schedule interval: 6 hours (period / 4)

Every 6 hours, schedules the next 4 daily executions:
- 2026-02-08 02:00:00
- 2026-02-09 02:00:00
- 2026-02-10 02:00:00
- 2026-02-11 02:00:00
```

## Why It Works and Is Reliable

### Correctness Guarantees

1. **Exactly-once delivery (with acknowledgment):**
   - Messages are locked during processing via `lockUuid`
   - Only deleted when acknowledged
   - Timeout mechanism ensures redelivery if consumer fails

2. **No lost messages:**
   - All operations are transactional
   - Database durability (WAL in PostgreSQL)
   - Retries on transient failures

3. **No duplicate processing (within acquisition window):**
   - FOR UPDATE SKIP LOCKED ensures different consumers get different messages
   - lockUuid prevents concurrent acquisition
   - Optimistic locking (CAS) prevents race conditions

4. **Ordering:**
   - Messages are processed in `scheduledAt` order (ORDER BY)
   - Within the same scheduledAt, ordering is not guaranteed

### Reliability Features

1. **Transaction-safe:**
   - All operations use database transactions
   - Atomicity: Either the entire operation succeeds or rolls back
   - Consistency: Constraints are always enforced

2. **Crash-safe:**
   - Database WAL ensures durability
   - Uncommitted transactions are rolled back automatically
   - Messages in-flight (locked) are released after timeout

3. **Retry logic:**
   - Transient failures (deadlocks, connection errors) are retried
   - CAS failures (concurrent modifications) are retried
   - Configurable retry policy with exponential backoff

4. **Timeout and redelivery:**
   - Acquired messages have a timeout (`acquireTimeout`)
   - If not acknowledged, they become available again
   - Prevents lost messages due to consumer crashes

### CronService Reliability

1. **Configuration changes are atomic:**
   - Old messages are deleted
   - New messages are inserted
   - All in a single `installTick()` call (uses transactions internally)

2. **No duplicate cron executions:**
   - Message keys include timestamp
   - Unique constraint prevents duplicates
   - ON CONFLICT DO NOTHING handles races between scheduler instances

3. **Graceful degradation:**
   - If database is unavailable during a scheduled tick, the operation fails
   - Next tick will try again
   - Messages already in the database remain available

4. **Multiple scheduler instances:**
   - Multiple processes can call `install()` for the same cron
   - The first one to insert wins (ON CONFLICT DO NOTHING)
   - DELETE operations are idempotent
   - Last writer wins for configuration changes

### PostgreSQL-Specific Features

1. **FOR UPDATE SKIP LOCKED:**
   - Available in PostgreSQL 9.5+
   - Critical for lock-free multi-consumer polling
   - Without this, consumers would serialize (not scale)

2. **ON CONFLICT DO NOTHING:**
   - Available in PostgreSQL 9.5+
   - Atomic insert-or-ignore
   - Prevents duplicate key exceptions

3. **SERIALIZABLE vs READ COMMITTED:**
   - Uses READ COMMITTED (default)
   - Faster than SERIALIZABLE
   - Row-level locks provide sufficient isolation
   - Retry loops handle concurrent modifications

4. **Indexes:**
   - Unique index on (pKey, pKind): Enforces uniqueness, speeds up lookups
   - Index on (pKind, scheduledAt): Critical for polling performance
   - Index on (lockUuid, id): Speeds up batch retrieval and acknowledgment

### Failure Scenarios and Recovery

1. **Consumer crashes after polling, before acknowledging:**
   - Message's scheduledAt expires (e.g., +5 minutes)
   - Message becomes available for redelivery
   - Another consumer can poll it

2. **Database connection lost during transaction:**
   - Transaction is rolled back automatically
   - No partial updates
   - Retry logic reconnects and retries

3. **Multiple consumers poll simultaneously:**
   - FOR UPDATE SKIP LOCKED ensures each gets a different message
   - No blocking, no duplicates

4. **Scheduler instance crashes:**
   - Messages already scheduled remain in the database
   - Another instance (or restart) continues scheduling
   - No message loss

5. **Clock skew between application and database:**
   - All timestamps use the application's clock (`Instant.now(clock)`)
   - Consistent within a single application
   - Multiple applications should use synchronized clocks (NTP)

### Performance Considerations

1. **Batch operations:**
   - `offerBatch()` reduces round-trips to the database
   - `tryPollMany()` acquires multiple messages in one transaction
   - Chunking prevents excessive memory usage

2. **Index usage:**
   - Queries use indexes effectively
   - EXPLAIN ANALYZE shows index scans (not table scans)

3. **Connection pooling:**
   - HikariCP provides efficient connection management
   - Reduces connection overhead

4. **Lock contention:**
   - SKIP LOCKED minimizes lock contention
   - Each consumer gets a different message
   - Scales linearly with consumers (within reason)

### Limitations and Trade-offs

1. **Redelivery can occur:**
   - If a consumer takes longer than `acquireTimeout` but eventually acknowledges
   - The message may be redelivered to another consumer
   - Application must handle idempotency

2. **Ordering within the same scheduledAt:**
   - Not guaranteed
   - Use different scheduledAt values for strict ordering

3. **CronService scheduler is in-process:**
   - Not distributed (each process has its own scheduler)
   - Database ensures no duplicate messages
   - But: If all processes crash, no new messages are scheduled until restart

4. **Database as a bottleneck:**
   - All operations go through the database
   - Database performance limits throughput
   - Scaling: Use read replicas, partitioning, or multiple queues

5. **Clock dependency:**
   - Relies on application clocks for scheduling
   - Clock skew can cause early/late delivery
   - Use NTP to synchronize clocks

## Summary

The DelayedQueue and CronService implementations provide:

- **Strong consistency:** ACID transactions, unique constraints, row-level locks
- **High concurrency:** FOR UPDATE SKIP LOCKED, optimistic locking, batch operations
- **Reliability:** Timeout/redelivery, retry logic, crash recovery
- **Simplicity:** Standard SQL, no external dependencies, no distributed consensus

The key insight is leveraging PostgreSQL's advanced locking features (FOR UPDATE SKIP LOCKED) and conflict resolution (ON CONFLICT) to build a concurrent, reliable message queue without complex distributed coordination.
