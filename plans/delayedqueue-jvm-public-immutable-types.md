# Initial Scala data structures

The following data types from the `delayedqueue-jvm` subproject should be mirrored in Scala, in subproject `delayedqueue-scala`.

Requirements:
- They need to be somewhat adapted to Scala idioms, although there isn't much to do (since the original inspiration was Scala code and these data structures are pretty much compatible with Scala)
  - Prefer `Option` for return types, and "nullable" types for input (e.g., `Type | Null`)
- If the original data structures have comments, those comments need to be kept (adapted to Scala and Scaladoc)
- We need back and forth conversions to and from the JVM types, so provide `asJava` and `asScala` extensions
- When side-effects are involved, e.g., the `acknowledge` callback, `IO` must be used.

## Configs

- `RetryConfig` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/RetryConfig.kt`
  - Exponential-backoff retry policy for DB operations; `data class` with `DEFAULT`/`NO_RETRIES`.
- `JdbcDatabasePoolConfig` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/JdbcDatabasePoolConfig.kt`
  - Connection-pool tuning options for Hikari (timeouts, pool sizes); immutable `data class`.
- `DelayedQueueTimeConfig` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/DelayedQueueTimeConfig.kt`
  - Acquire/poll timing configuration with several provided defaults.
- `JdbcConnectionConfig` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/JdbcConnectionConfig.kt`
  - JDBC connection settings (url, driver, optional credentials and pool).
- `DelayedQueueJDBCConfig` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/DelayedQueueJDBCConfig.kt`
  - Composite config for creating JDBC-backed delayed queues (db, table, queueName, time, retryPolicy).

## Messages & Envelopes

- `ScheduledMessage<A>` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/ScheduledMessage.kt`
  - Message scheduled for future delivery (key, payload, scheduleAt, canUpdate).
- `BatchedMessage<In,A>` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/ScheduledMessage.kt`
  - Input wrapper for batch offering operations.
- `BatchedReply<In,A>` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/ScheduledMessage.kt`
  - Reply for batched offer with `OfferOutcome`.
- `AckEnvelope<A>` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/AckEnvelope.kt`
  - Envelope returned from polls with payload, `MessageId`, timestamp, `DeliveryType`, and `acknowledge()`.
- `MessageId` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/AckEnvelope.kt`
  - Simple wrapper `data class` for message id string.
- `DeliveryType` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/AckEnvelope.kt`
  - Enum: `FIRST_DELIVERY` / `REDELIVERY`.

## Cron / Scheduling

- `CronMessage<A>` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/CronMessage.kt`
  - Periodic/cron message wrapper; convertible to `ScheduledMessage` via `toScheduled(...)`.
- `CronConfigHash` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/CronConfigHash.kt`
  - Hash identifying cron configuration (MD5 string wrapper) used to detect config changes.
- `CronDailySchedule` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/CronDailySchedule.kt`
  - Timezone-aware daily schedule config (hours, scheduleInAdvance, scheduleInterval).

## Return / Outcome / Error types

- `OfferOutcome` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/OfferOutcome.kt`
  - Sealed interface with immutable data objects `Created` / `Updated` / `Ignored` — return of `offer*`.
- `ResourceUnavailableException` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/exceptions.kt`
  - Checked exception type (subclass of `Exception`) used when resources (DB) are unavailable.
- `JdbcDriver` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/JdbcDriver.kt`
  - Immutable holder for driver `className` and canonical `JvmField` entries for supported drivers.
- `AcknowledgeFun` — `delayedqueue-jvm/src/main/kotlin/org/funfix/delayedqueue/jvm/AckEnvelope.kt`
  - `fun interface` used as the acknowledge callback inside `AckEnvelope` (functional immutable reference).
