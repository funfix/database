package org.funfix.delayedqueue.jvm

import java.security.MessageDigest
import java.sql.SQLException
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.funfix.delayedqueue.jvm.internals.jdbc.DBTableRow
import org.funfix.delayedqueue.jvm.internals.jdbc.HSQLDBMigrations
import org.funfix.delayedqueue.jvm.internals.jdbc.MigrationRunner
import org.funfix.delayedqueue.jvm.internals.jdbc.SQLVendorAdapter
import org.funfix.delayedqueue.jvm.internals.utils.Database
import org.funfix.delayedqueue.jvm.internals.utils.sneakyRaises
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
 * - Automatic schema migrations
 * - Vendor-specific query optimizations
 *
 * ## Java Usage
 *
 * ```java
 * JdbcConnectionConfig config = new JdbcConnectionConfig(
 *     "jdbc:hsqldb:mem:testdb",
 *     JdbcDriver.HSQLDB,
 *     null, // username
 *     null, // password
 *     null  // pool config
 * );
 *
 * DelayedQueue<String> queue = DelayedQueueJDBC.create(
 *     config,
 *     "my_delayed_queue_table",
 *     MessageSerializer.forStrings()
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
    private val timeConfig: DelayedQueueTimeConfig,
    private val clock: Clock,
    private val tableName: String,
    private val pKind: String,
    private val ackEnvSource: String,
) : DelayedQueue<A>, AutoCloseable {
    private val logger = LoggerFactory.getLogger(DelayedQueueJDBC::class.java)
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    override fun getTimeConfig(): DelayedQueueTimeConfig = timeConfig

    @Throws(SQLException::class, InterruptedException::class)
    override fun offerOrUpdate(key: String, payload: A, scheduleAt: Instant): OfferOutcome =
        offer(key, payload, scheduleAt, canUpdate = true)

    @Throws(SQLException::class, InterruptedException::class)
    override fun offerIfNotExists(key: String, payload: A, scheduleAt: Instant): OfferOutcome =
        offer(key, payload, scheduleAt, canUpdate = false)

    @Throws(SQLException::class, InterruptedException::class)
    private fun offer(
        key: String,
        payload: A,
        scheduleAt: Instant,
        canUpdate: Boolean,
    ): OfferOutcome = sneakyRaises {
        database.withTransaction { connection ->
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
                        scheduledAtInitially = existing.data.scheduledAtInitially,
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

    @Throws(SQLException::class, InterruptedException::class)
    override fun <In> offerBatch(messages: List<BatchedMessage<In, A>>): List<BatchedReply<In, A>> =
        sneakyRaises {
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
                    } catch (e: SQLException) {
                        // Batch insert failed, fall back to individual inserts
                        logger.warn("Batch insert failed, falling back to individual inserts", e)
                        toInsert.forEach { msg -> results[msg.message.key] = OfferOutcome.Ignored }
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
            messages.map { msg ->
                BatchedReply(
                    input = msg.input,
                    message = msg.message,
                    outcome = results[msg.message.key] ?: OfferOutcome.Ignored,
                )
            }
        }

    @Throws(SQLException::class, InterruptedException::class)
    override fun tryPoll(): AckEnvelope<A>? = sneakyRaises {
        database.withTransaction { connection ->
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
                    timeConfig.acquireTimeout,
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
                source = ackEnvSource,
                deliveryType = deliveryType,
                acknowledge =
                    AcknowledgeFun {
                        try {
                            sneakyRaises {
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

    @Throws(SQLException::class, InterruptedException::class)
    override fun tryPollMany(batchMaxSize: Int): AckEnvelope<List<A>> = sneakyRaises {
        database.withTransaction { connection ->
            val now = Instant.now(clock)
            val lockUuid = UUID.randomUUID().toString()

            val count =
                adapter.acquireManyOptimistically(
                    connection.underlying,
                    pKind,
                    batchMaxSize,
                    lockUuid,
                    timeConfig.acquireTimeout,
                    now,
                )

            if (count == 0) {
                return@withTransaction AckEnvelope(
                    payload = emptyList(),
                    messageId = MessageId(lockUuid),
                    timestamp = now,
                    source = ackEnvSource,
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
                source = ackEnvSource,
                deliveryType = DeliveryType.FIRST_DELIVERY,
                acknowledge =
                    AcknowledgeFun {
                        try {
                            sneakyRaises {
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

    @Throws(SQLException::class, InterruptedException::class)
    override fun poll(): AckEnvelope<A> {
        while (true) {
            val result = tryPoll()
            if (result != null) {
                return result
            }

            // Wait for new messages
            lock.withLock {
                condition.await(timeConfig.pollPeriod.toMillis(), TimeUnit.MILLISECONDS)
            }
        }
    }

    @Throws(SQLException::class, InterruptedException::class)
    override fun read(key: String): AckEnvelope<A>? = sneakyRaises {
        database.withConnection { connection ->
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
                source = ackEnvSource,
                deliveryType = deliveryType,
                acknowledge =
                    AcknowledgeFun {
                        try {
                            sneakyRaises {
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

    @Throws(SQLException::class, InterruptedException::class)
    override fun dropMessage(key: String): Boolean = sneakyRaises {
        database.withTransaction { connection ->
            adapter.deleteOneRow(connection.underlying, key, pKind)
        }
    }

    @Throws(SQLException::class, InterruptedException::class)
    override fun containsMessage(key: String): Boolean = sneakyRaises {
        database.withConnection { connection ->
            adapter.checkIfKeyExists(connection.underlying, key, pKind)
        }
    }

    @Throws(SQLException::class, InterruptedException::class)
    override fun dropAllMessages(confirm: String): Int {
        require(confirm == "Yes, please, I know what I'm doing!") {
            "To drop all messages, you must provide the exact confirmation string"
        }

        return sneakyRaises {
            database.withTransaction { connection ->
                adapter.dropAllMessages(connection.underlying, pKind)
            }
        }
    }

    override fun getCron(): CronService<A> = cronService

    private val cronService: CronService<A> by lazy {
        org.funfix.delayedqueue.jvm.internals.CronServiceImpl(
            queue = this,
            clock = clock,
            deleteCurrentCron = { configHash, keyPrefix ->
                sneakyRaises {
                    database.withTransaction { connection ->
                        adapter.deleteCurrentCron(
                            connection.underlying,
                            pKind,
                            keyPrefix,
                            configHash.value,
                        )
                    }
                }
            },
            deleteOldCron = { configHash, keyPrefix ->
                sneakyRaises {
                    database.withTransaction { connection ->
                        adapter.deleteOldCron(
                            connection.underlying,
                            pKind,
                            keyPrefix,
                            configHash.value,
                        )
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
         * Creates a new JDBC-based delayed queue with default configuration.
         *
         * @param A the type of message payloads
         * @param connectionConfig JDBC connection configuration
         * @param tableName the name of the database table to use
         * @param serializer strategy for serializing/deserializing message payloads
         * @param timeConfig optional time configuration (uses defaults if not provided)
         * @param clock optional clock for time operations (uses system UTC if not provided)
         * @return a new DelayedQueueJDBC instance
         * @throws SQLException if database initialization fails
         */
        @JvmStatic
        @JvmOverloads
        @Throws(SQLException::class, InterruptedException::class)
        public fun <A> create(
            connectionConfig: JdbcConnectionConfig,
            tableName: String,
            serializer: MessageSerializer<A>,
            timeConfig: DelayedQueueTimeConfig = DelayedQueueTimeConfig.DEFAULT,
            clock: Clock = Clock.systemUTC(),
        ): DelayedQueueJDBC<A> = sneakyRaises {
            val database = Database(connectionConfig)

            // Run migrations
            database.withConnection { connection ->
                val migrations =
                    when (connectionConfig.driver) {
                        JdbcDriver.HSQLDB -> HSQLDBMigrations.getMigrations(tableName)
                        JdbcDriver.MsSqlServer,
                        JdbcDriver.Sqlite ->
                            throw UnsupportedOperationException(
                                "Database ${connectionConfig.driver} not yet supported"
                            )
                    }

                val executed = MigrationRunner.runMigrations(connection.underlying, migrations)
                if (executed > 0) {
                    logger.info("Executed $executed migrations for table $tableName")
                }
            }

            val adapter = SQLVendorAdapter.create(connectionConfig.driver, tableName)

            // Generate pKind as MD5 hash of type name (for partitioning)
            val pKind = computePartitionKind(serializer.javaClass.name)

            DelayedQueueJDBC(
                database = database,
                adapter = adapter,
                serializer = serializer,
                timeConfig = timeConfig,
                clock = clock,
                tableName = tableName,
                pKind = pKind,
                ackEnvSource = "DelayedQueueJDBC:$tableName",
            )
        }

        private fun computePartitionKind(typeName: String): String {
            val md5 = MessageDigest.getInstance("MD5")
            val digest = md5.digest(typeName.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
