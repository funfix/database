package org.funfix.delayedqueue.jvm

import java.security.MessageDigest
import java.sql.SQLException
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.funfix.delayedqueue.jvm.internals.CronServiceImpl
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.HSQLDBMigrations
import org.funfix.delayedqueue.jvm.internals.jdbc.MigrationRunner
import org.funfix.delayedqueue.jvm.internals.jdbc.RdbmsExceptionFilters
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.jdbc.filtersForDriver
import org.funfix.delayedqueue.jvm.internals.jdbc.withDbRetries
import org.funfix.delayedqueue.jvm.internals.utils.Database
import org.funfix.delayedqueue.jvm.internals.utils.Raise
import org.funfix.delayedqueue.jvm.internals.utils.unsafeSneakyRaises
import org.funfix.delayedqueue.jvm.internals.utils.withConnection
import org.funfix.delayedqueue.jvm.internals.utils.withTransaction
import org.slf4j.LoggerFactory

/**
 * JDBC-based implementation of [DelayedQueue] with support for multiple database backends.
 *
 * This implementation stores messages in a relational database table and supports vendor-specific
 * optimizations for different databases (HSQLDB, MS-SQL, SQLite, PostgreSQL).
 *
 * ## Features
 * - Persistent storage in relational databases
 * - Optimistic locking for concurrent message acquisition
 * - Batch operations for improved performance
 * - Vendor-specific query optimizations
 *
 * ## Java Usage
 *
 * ```java
 * JdbcConnectionConfig dbConfig = new JdbcConnectionConfig(
 *     "jdbc:hsqldb:mem:testdb",
 *     JdbcDriver.HSQLDB,
 *     null, // username
 *     null, // password
 *     null  // pool config
 * );
 *
 * DelayedQueueJDBCConfig config = DelayedQueueJDBCConfig.create(
 *     dbConfig,
 *     "delayed_queue_table",
 *     "my-queue"
 * );
 *
 * // Run migrations explicitly (do this once, not on every queue creation)
 * DelayedQueueJDBC.runMigrations(config);
 *
 * DelayedQueue<String> queue = DelayedQueueJDBC.create(
 *     MessageSerializer.forStrings(),
 *     config
 * );
 * ```
 *
 * @param A the type of message payloads
 */
public class DelayedQueueJDBC<A>
private constructor(
    private val database: Database,
    private val adapter: SQLVendorAdapter,
    private val serializer: MessageSerializer<A>,
    private val config: DelayedQueueJDBCConfig,
    private val clock: Clock,
) : DelayedQueue<A>, AutoCloseable {
    private val logger = LoggerFactory.getLogger(DelayedQueueJDBC::class.java)
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    private val pKind: String =
        computePartitionKind("${config.queueName}|${serializer.getTypeName()}")

    /** Exception filters based on the JDBC driver being used. */
    private val filters: RdbmsExceptionFilters = filtersForDriver(adapter.driver)

    override fun getTimeConfig(): DelayedQueueTimeConfig = config.time

    /**
     * Wraps database operations with retry logic based on configuration.
     *
     * If retryPolicy is null, executes the block directly. Otherwise, applies retry logic with
     * database-specific exception filtering.
     *
     * This method has Raise context for ResourceUnavailableException and InterruptedException,
     * which matches what the public API declares via @Throws.
     */
    context(_: Raise<ResourceUnavailableException>, _: Raise<InterruptedException>)
    private fun <T> withRetries(block: () -> T): T {
        return if (config.retryPolicy == null) {
            block()
        } else {
            withDbRetries(
                config = config.retryPolicy,
                clock = clock,
                filters = filters,
                block = block,
            )
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun offerOrUpdate(key: String, payload: A, scheduleAt: Instant): OfferOutcome =
        unsafeSneakyRaises {
            withRetries { offer(key, payload, scheduleAt, canUpdate = true) }
        }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun offerIfNotExists(key: String, payload: A, scheduleAt: Instant): OfferOutcome =
        unsafeSneakyRaises {
            withRetries { offer(key, payload, scheduleAt, canUpdate = false) }
        }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    private fun offer(
        key: String,
        payload: A,
        scheduleAt: Instant,
        canUpdate: Boolean,
    ): OfferOutcome {
        val now = Instant.now(clock)
        val serialized = serializer.serialize(payload)
        val newRow =
            DBTableRow(
                pKey = key,
                pKind = pKind,
                payload = serialized,
                scheduledAt = scheduleAt,
                scheduledAtInitially = scheduleAt,
                lockUuid = null,
                createdAt = now,
            )

        // Step 1: Optimistic INSERT (in its own transaction)
        // This matches the original Scala implementation's approach:
        // Try to insert first, and only SELECT+UPDATE if the insert fails
        val inserted =
            database.withTransaction { connection ->
                adapter.insertOneRow(connection.underlying, newRow)
            }

        if (inserted) {
            lock.withLock { condition.signalAll() }
            return OfferOutcome.Created
        }

        // INSERT failed - key already exists
        if (!canUpdate) {
            return OfferOutcome.Ignored
        }

        // Step 2: Retry loop for SELECT FOR UPDATE + UPDATE (in single transaction)
        // This matches the Scala implementation which retries on concurrent modification
        while (true) {
            val outcome =
                database.withTransaction { connection ->
                    // Use locking SELECT to prevent concurrent modifications
                    val existing =
                        adapter.selectForUpdateOneRow(connection.underlying, pKind, key)
                            ?: return@withTransaction null // Row disappeared, retry

                    // Check if the row is a duplicate
                    if (existing.data.isDuplicate(newRow)) {
                        return@withTransaction OfferOutcome.Ignored
                    }

                    // Try to update with guarded CAS (compare-and-swap)
                    val updated =
                        adapter.guardedUpdate(connection.underlying, existing.data, newRow)
                    if (updated) {
                        OfferOutcome.Updated
                    } else {
                        null // CAS failed, retry
                    }
                }

            // If outcome is not null, we succeeded (either Updated or Ignored)
            if (outcome != null) {
                if (outcome is OfferOutcome.Updated) {
                    lock.withLock { condition.signalAll() }
                }
                return outcome
            }

            // outcome was null, which means we need to retry (concurrent modification)
            // Loop back and try again
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun <In> offerBatch(messages: List<BatchedMessage<In, A>>): List<BatchedReply<In, A>> =
        unsafeSneakyRaises {
            withRetries { offerBatchImpl(messages) }
        }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    private fun <In> offerBatchImpl(
        messages: List<BatchedMessage<In, A>>
    ): List<BatchedReply<In, A>> {
        if (messages.isEmpty()) {
            return emptyList()
        }

        val now = Instant.now(clock)

        // Step 1: Try batch INSERT (optimistic)
        // This matches the original Scala implementation's insertMany function
        val insertOutcomes: Map<String, OfferOutcome> =
            database.withTransaction { connection ->
                // Find existing keys in a SINGLE query (not N queries)
                val keys = messages.map { it.message.key }
                val existingKeys = adapter.searchAvailableKeys(connection.underlying, pKind, keys)

                // Filter to only insert non-existing keys
                val rowsToInsert =
                    messages
                        .filter { !existingKeys.contains(it.message.key) }
                        .map { msg ->
                            DBTableRow(
                                pKey = msg.message.key,
                                pKind = pKind,
                                payload = serializer.serialize(msg.message.payload),
                                scheduledAt = msg.message.scheduleAt,
                                scheduledAtInitially = msg.message.scheduleAt,
                                lockUuid = null,
                                createdAt = now,
                            )
                        }

                // Attempt batch insert
                if (rowsToInsert.isEmpty()) {
                    // All keys already exist
                    messages.associate { it.message.key to OfferOutcome.Ignored }
                } else {
                    try {
                        val inserted = adapter.insertBatch(connection.underlying, rowsToInsert)
                        if (inserted.isNotEmpty()) {
                            lock.withLock { condition.signalAll() }
                        }

                        // Build outcome map: Created for inserted, Ignored for existing
                        messages.associate { msg ->
                            if (existingKeys.contains(msg.message.key)) {
                                msg.message.key to OfferOutcome.Ignored
                            } else if (inserted.contains(msg.message.key)) {
                                msg.message.key to OfferOutcome.Created
                            } else {
                                // Failed to insert (shouldn't happen with no exception, but be
                                // safe)
                                msg.message.key to OfferOutcome.Ignored
                            }
                        }
                    } catch (e: Exception) {
                        // On duplicate key, return empty map to trigger one-by-one fallback
                        // This matches: recover { case SQLExceptionExtractors.DuplicateKey(_) =>
                        // Map.empty }
                        when {
                            filters.duplicateKey.matches(e) -> {
                                logger.debug(
                                    "Batch insert failed due to duplicate key (concurrent insert), " +
                                        "falling back to one-by-one offers"
                                )
                                emptyMap() // Trigger fallback
                            }
                            else -> throw e // Other exceptions propagate
                        }
                    }
                }
            }

        // Step 2: Fallback to one-by-one for failures or updates
        // This matches the Scala implementation's fallback logic
        val needsRetry =
            messages.filter { msg ->
                when (val outcome = insertOutcomes[msg.message.key]) {
                    null -> true // Error/not in map, retry
                    is OfferOutcome.Ignored -> msg.message.canUpdate // Needs update
                    else -> false // Created successfully
                }
            }

        val results = insertOutcomes.toMutableMap()

        // Call offer() one-by-one for messages that need retry or update
        needsRetry.forEach { msg ->
            val outcome =
                offer(
                    msg.message.key,
                    msg.message.payload,
                    msg.message.scheduleAt,
                    canUpdate = msg.message.canUpdate,
                )
            results[msg.message.key] = outcome
        }

        // Create replies
        return messages.map { msg ->
            BatchedReply(
                input = msg.input,
                message = msg.message,
                outcome = results[msg.message.key] ?: OfferOutcome.Ignored,
            )
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun tryPoll(): AckEnvelope<A>? = unsafeSneakyRaises { withRetries { tryPollImpl() } }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    private fun tryPollImpl(): AckEnvelope<A>? {
        // Retry loop to handle failed acquires (concurrent modifications)
        // This matches the original Scala implementation which retries if acquire fails
        while (true) {
            val envelope =
                database.withTransaction { connection ->
                    val now = Instant.now(clock)
                    val lockUuid = UUID.randomUUID().toString()

                    // Select first available message (with locking if supported by DB)
                    val row =
                        adapter.selectFirstAvailableWithLock(connection.underlying, pKind, now)
                            ?: return@withTransaction null // No messages available

                    // Try to acquire the row by updating it with our lock
                    val acquired =
                        adapter.acquireRowByUpdate(
                            connection.underlying,
                            row.data,
                            lockUuid,
                            config.time.acquireTimeout,
                            now,
                        )

                    if (!acquired) {
                        // Concurrent modification - another thread acquired this row
                        // Signal retry by returning a special marker (empty envelope with null
                        // payload)
                        // We'll check for this below and continue the loop
                        return@withTransaction AckEnvelope<A?>(
                            payload = null,
                            messageId = MessageId("__RETRY__"),
                            timestamp = now,
                            source = "",
                            deliveryType = DeliveryType.FIRST_DELIVERY,
                            acknowledge = {},
                        )
                    }

                    // Successfully acquired the message
                    val payload = serializer.deserialize(row.data.payload)
                    val deliveryType =
                        if (row.data.scheduledAtInitially.isBefore(row.data.scheduledAt)) {
                            DeliveryType.REDELIVERY
                        } else {
                            DeliveryType.FIRST_DELIVERY
                        }

                    AckEnvelope(
                        payload = payload,
                        messageId = MessageId(row.data.pKey),
                        timestamp = now,
                        source = config.ackEnvSource,
                        deliveryType = deliveryType,
                        acknowledge = {
                            try {
                                unsafeSneakyRaises {
                                    database.withTransaction { ackConn ->
                                        adapter.deleteRowsWithLock(ackConn.underlying, lockUuid)
                                    }
                                }
                            } catch (e: Exception) {
                                logger.warn("Failed to acknowledge message with lock $lockUuid", e)
                            }
                        },
                    )
                }

            // Check if we should retry (null payload means retry marker)
            if (envelope == null) {
                return null // No messages available
            } else if (envelope.payload == null) {
                continue // Retry marker, try next message
            } else {
                @Suppress("UNCHECKED_CAST")
                return envelope as AckEnvelope<A>
            }
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun tryPollMany(batchMaxSize: Int): AckEnvelope<List<A>> = unsafeSneakyRaises {
        withRetries { tryPollManyImpl(batchMaxSize) }
    }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    private fun tryPollManyImpl(batchMaxSize: Int): AckEnvelope<List<A>> {
        // Handle edge case: non-positive batch size
        if (batchMaxSize <= 0) {
            val now = Instant.now(clock)
            return AckEnvelope(
                payload = emptyList(),
                messageId = MessageId(UUID.randomUUID().toString()),
                timestamp = now,
                source = config.ackEnvSource,
                deliveryType = DeliveryType.FIRST_DELIVERY,
                acknowledge = AcknowledgeFun {},
            )
        }

        return database.withTransaction { connection ->
            val now = Instant.now(clock)
            val lockUuid = UUID.randomUUID().toString()

            val count =
                adapter.acquireManyOptimistically(
                    connection.underlying,
                    pKind,
                    batchMaxSize,
                    lockUuid,
                    config.time.acquireTimeout,
                    now,
                )

            if (count == 0) {
                return@withTransaction AckEnvelope(
                    payload = emptyList(),
                    messageId = MessageId(lockUuid),
                    timestamp = now,
                    source = config.ackEnvSource,
                    deliveryType = DeliveryType.FIRST_DELIVERY,
                    acknowledge = AcknowledgeFun {},
                )
            }

            val rows =
                adapter.selectAllAvailableWithLock(connection.underlying, lockUuid, count, null)

            val payloads = rows.map { row -> serializer.deserialize(row.data.payload) }

            // Determine delivery type: if ALL rows have scheduledAtInitially < scheduledAt, it's a
            // redelivery
            val deliveryType =
                if (
                    rows.all { row -> row.data.scheduledAtInitially.isBefore(row.data.scheduledAt) }
                ) {
                    DeliveryType.REDELIVERY
                } else {
                    DeliveryType.FIRST_DELIVERY
                }

            AckEnvelope(
                payload = payloads,
                messageId = MessageId(lockUuid),
                timestamp = now,
                source = config.ackEnvSource,
                deliveryType = deliveryType,
                acknowledge = {
                    try {
                        unsafeSneakyRaises {
                            database.withTransaction { ackConn ->
                                adapter.deleteRowsWithLock(ackConn.underlying, lockUuid)
                            }
                        }
                    } catch (e: Exception) {
                        logger.warn("Failed to acknowledge batch with lock $lockUuid", e)
                    }
                },
            )
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun poll(): AckEnvelope<A> {
        while (true) {
            val result = tryPoll()
            if (result != null) {
                return result
            }

            // Wait for new messages
            lock.withLock {
                condition.await(config.time.pollPeriod.toMillis(), TimeUnit.MILLISECONDS)
            }
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun read(key: String): AckEnvelope<A>? = unsafeSneakyRaises {
        withRetries { readImpl(key) }
    }

    context(_: Raise<InterruptedException>, _: Raise<SQLException>)
    private fun readImpl(key: String): AckEnvelope<A>? {
        return database.withConnection { connection ->
            val row =
                adapter.selectByKey(connection.underlying, pKind, key) ?: return@withConnection null

            val payload = serializer.deserialize(row.data.payload)
            val now = Instant.now(clock)

            val deliveryType =
                if (row.data.scheduledAtInitially.isBefore(row.data.scheduledAt)) {
                    DeliveryType.REDELIVERY
                } else {
                    DeliveryType.FIRST_DELIVERY
                }

            AckEnvelope(
                payload = payload,
                messageId = MessageId(row.data.pKey),
                timestamp = now,
                source = config.ackEnvSource,
                deliveryType = deliveryType,
                acknowledge = {
                    try {
                        unsafeSneakyRaises {
                            database.withTransaction { ackConn ->
                                adapter.deleteRowByFingerprint(ackConn.underlying, row)
                            }
                        }
                    } catch (e: Exception) {
                        logger.warn("Failed to acknowledge message $key", e)
                    }
                },
            )
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun dropMessage(key: String): Boolean = unsafeSneakyRaises {
        withRetries {
            database.withTransaction { connection ->
                adapter.deleteOneRow(connection.underlying, key, pKind)
            }
        }
    }

    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    override fun containsMessage(key: String): Boolean = unsafeSneakyRaises {
        withRetries {
            database.withConnection { connection ->
                adapter.checkIfKeyExists(connection.underlying, key, pKind)
            }
        }
    }

    @Throws(
        IllegalArgumentException::class,
        ResourceUnavailableException::class,
        InterruptedException::class,
    )
    override fun dropAllMessages(confirm: String): Int {
        require(confirm == "Yes, please, I know what I'm doing!") {
            "To drop all messages, you must provide the exact confirmation string"
        }

        return unsafeSneakyRaises {
            withRetries {
                database.withTransaction { connection ->
                    adapter.dropAllMessages(connection.underlying, pKind)
                }
            }
        }
    }

    override fun getCron(): CronService<A> = cronService

    private val cronService: CronService<A> by lazy {
        CronServiceImpl(
            queue = this,
            clock = clock,
            deleteCurrentCron = { configHash, keyPrefix ->
                unsafeSneakyRaises {
                    withRetries {
                        database.withTransaction { connection ->
                            adapter.deleteCurrentCron(
                                connection.underlying,
                                pKind,
                                keyPrefix,
                                configHash.value,
                            )
                        }
                    }
                }
            },
            deleteOldCron = { configHash, keyPrefix ->
                unsafeSneakyRaises {
                    withRetries {
                        database.withTransaction { connection ->
                            adapter.deleteOldCron(
                                connection.underlying,
                                pKind,
                                keyPrefix,
                                configHash.value,
                            )
                        }
                    }
                }
            },
        )
    }

    override fun close() {
        database.close()
    }

    public companion object {
        private val logger = LoggerFactory.getLogger(DelayedQueueJDBC::class.java)

        /**
         * Runs database migrations for the specified configuration.
         *
         * This should be called explicitly before creating a DelayedQueueJDBC instance. Running
         * migrations automatically on every queue creation is discouraged.
         *
         * @param config queue configuration containing database connection and table settings
         * @throws ResourceUnavailableException if database connection fails
         * @throws InterruptedException if interrupted during migration
         */
        @JvmStatic
        @Throws(ResourceUnavailableException::class, InterruptedException::class)
        public fun runMigrations(config: DelayedQueueJDBCConfig): Unit = unsafeSneakyRaises {
            val database = Database(config.db)
            database.use {
                database.withConnection { connection ->
                    val migrations =
                        when (config.db.driver) {
                            JdbcDriver.HSQLDB -> HSQLDBMigrations.getMigrations(config.tableName)
                            JdbcDriver.MsSqlServer,
                            JdbcDriver.Sqlite ->
                                throw UnsupportedOperationException(
                                    "Database ${config.db.driver} not yet supported"
                                )
                        }

                    val executed = MigrationRunner.runMigrations(connection.underlying, migrations)
                    if (executed > 0) {
                        logger.info("Executed $executed migrations for table ${config.tableName}")
                    }
                }
            }
        }

        /**
         * Creates a new JDBC-based delayed queue with the specified configuration.
         *
         * NOTE: This method does NOT run database migrations automatically. You must call
         * [runMigrations] explicitly before creating the queue.
         *
         * @param A the type of message payloads
         * @param serializer strategy for serializing/deserializing message payloads
         * @param config configuration for this queue instance (db, table, time, queue name, retry
         *   policy)
         * @param clock optional clock for time operations (uses system UTC if not provided)
         * @return a new DelayedQueueJDBC instance
         * @throws ResourceUnavailableException if database initialization fails
         * @throws InterruptedException if interrupted during initialization
         */
        @JvmStatic
        @JvmOverloads
        @Throws(ResourceUnavailableException::class, InterruptedException::class)
        public fun <A> create(
            serializer: MessageSerializer<A>,
            config: DelayedQueueJDBCConfig,
            clock: Clock = Clock.systemUTC(),
        ): DelayedQueueJDBC<A> = unsafeSneakyRaises {
            val database = Database(config.db)
            val adapter = SQLVendorAdapter.create(config.db.driver, config.tableName)
            DelayedQueueJDBC(
                database = database,
                adapter = adapter,
                serializer = serializer,
                config = config,
                clock = clock,
            )
        }

        private fun computePartitionKind(typeName: String): String {
            val md5 = MessageDigest.getInstance("MD5")
            val digest = md5.digest(typeName.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
