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
        return database.withTransaction { connection ->
            val existing = adapter.selectByKey(connection.underlying, pKind, key)
            val now = Instant.now(clock)
            val serialized = serializer.serialize(payload)

            if (existing != null) {
                if (!canUpdate) {
                    return@withTransaction OfferOutcome.Ignored
                }

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

                if (existing.data.isDuplicate(newRow)) {
                    OfferOutcome.Ignored
                } else {
                    val updated =
                        adapter.guardedUpdate(connection.underlying, existing.data, newRow)
                    if (updated) {
                        lock.withLock { condition.signalAll() }
                        OfferOutcome.Updated
                    } else {
                        // Concurrent modification, retry
                        OfferOutcome.Ignored
                    }
                }
            } else {
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

                val inserted = adapter.insertOneRow(connection.underlying, newRow)
                if (inserted) {
                    lock.withLock { condition.signalAll() }
                    OfferOutcome.Created
                } else {
                    // Key already exists due to concurrent insert
                    OfferOutcome.Ignored
                }
            }
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
        val now = Instant.now(clock)

        // Separate into insert and update batches
        val (toInsert, toUpdate) =
            messages.partition { msg ->
                !database.withConnection { connection ->
                    adapter.checkIfKeyExists(connection.underlying, msg.message.key, pKind)
                }
            }

        val results = mutableMapOf<String, OfferOutcome>()

        // Try batched inserts first
        if (toInsert.isNotEmpty()) {
            database.withTransaction { connection ->
                val rows =
                    toInsert.map { msg ->
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

                try {
                    val inserted = adapter.insertBatch(connection.underlying, rows)
                    inserted.forEach { key -> results[key] = OfferOutcome.Created }

                    // Mark non-inserted as ignored
                    toInsert.forEach { msg ->
                        if (msg.message.key !in inserted) {
                            results[msg.message.key] = OfferOutcome.Ignored
                        }
                    }

                    if (inserted.isNotEmpty()) {
                        lock.withLock { condition.signalAll() }
                    }
                } catch (e: Exception) {
                    // CRITICAL: Only catch duplicate key exceptions and fallback to individual
                    // inserts.
                    // All other exceptions should propagate up to the retry logic.
                    // This matches the original Scala implementation which uses
                    // `recover { case SQLExceptionExtractors.DuplicateKey(_) => ... }`
                    when {
                        filters.duplicateKey.matches(e) -> {
                            // A concurrent insert happened, and we don't know which keys are
                            // duplicated.
                            // Due to concurrency, it's safer to try inserting them one by one.
                            logger.warn(
                                "Batch insert failed due to duplicate key violation, " +
                                    "falling back to individual inserts",
                                e,
                            )
                            // Mark all as ignored; they'll be retried individually below
                            toInsert.forEach { msg ->
                                results[msg.message.key] = OfferOutcome.Ignored
                            }
                        }
                        else -> {
                            // Not a duplicate key exception - this is an unexpected error
                            // that should be handled by retry logic or fail fast
                            throw e
                        }
                    }
                }
            }
        }

        // Handle updates individually
        toUpdate.forEach { msg ->
            if (msg.message.canUpdate) {
                val outcome =
                    offer(
                        msg.message.key,
                        msg.message.payload,
                        msg.message.scheduleAt,
                        canUpdate = true,
                    )
                results[msg.message.key] = outcome
            } else {
                results[msg.message.key] = OfferOutcome.Ignored
            }
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
        return database.withTransaction { connection ->
            val now = Instant.now(clock)
            val lockUuid = UUID.randomUUID().toString()

            val row =
                adapter.selectFirstAvailableWithLock(connection.underlying, pKind, now)
                    ?: return@withTransaction null

            val acquired =
                adapter.acquireRowByUpdate(
                    connection.underlying,
                    row.data,
                    lockUuid,
                    config.time.acquireTimeout,
                    now,
                )

            if (!acquired) {
                return@withTransaction null
            }

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

            AckEnvelope(
                payload = payloads,
                messageId = MessageId(lockUuid),
                timestamp = now,
                source = config.ackEnvSource,
                deliveryType = DeliveryType.FIRST_DELIVERY,
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
