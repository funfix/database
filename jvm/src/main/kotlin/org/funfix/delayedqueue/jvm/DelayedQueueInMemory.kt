package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant
import java.util.ArrayList
import java.util.TreeSet
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * In-memory implementation of [DelayedQueue] using concurrent data structures.
 *
 * This implementation uses a [ReentrantLock] to protect mutable state (compatible with virtual
 * threads) and condition variables for efficient blocking in [poll].
 *
 * ## Java Usage
 *
 * ```java
 * DelayedQueue<String> queue = DelayedQueueInMemory.create();
 * queue.offerOrUpdate("key1", "Hello", Instant.now().plusSeconds(10));
 * AckEnvelope<String> msg = queue.poll();
 * ```
 *
 * @param A the type of message payloads
 */
public class DelayedQueueInMemory<A>
private constructor(
    private val timeConfig: DelayedQueueTimeConfig,
    private val ackEnvSource: String,
    private val clock: Clock,
) : DelayedQueue<A> {

    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    // Mutable state protected by lock
    private val map: MutableMap<String, Message<A>> = HashMap()
    private val order: TreeSet<Message<A>> = TreeSet(Message.comparator())

    override fun getTimeConfig(): DelayedQueueTimeConfig = timeConfig

    override fun offerOrUpdate(key: String, payload: A, scheduleAt: Instant): OfferOutcome {
        val now = clock.now()
        val msg =
            Message(
                key,
                payload,
                scheduleAt,
                scheduleAt,
                now,
                ackEnvSource,
                DeliveryType.FIRST_DELIVERY,
            )

        return lock.withLock {
            val existing = map[key]
            if (existing != null) {
                // Update case
                if (existing.isSameData(msg)) {
                    OfferOutcome.Ignored
                } else {
                    order.remove(existing)
                    map[key] = msg
                    order.add(msg)
                    condition.signalAll()
                    OfferOutcome.Updated
                }
            } else {
                // Create case
                map[key] = msg
                order.add(msg)
                condition.signalAll()
                OfferOutcome.Created
            }
        }
    }

    override fun offerIfNotExists(key: String, payload: A, scheduleAt: Instant): OfferOutcome {
        val now = clock.now()
        val msg =
            Message(
                key,
                payload,
                scheduleAt,
                scheduleAt,
                now,
                ackEnvSource,
                DeliveryType.FIRST_DELIVERY,
            )

        return lock.withLock {
            if (map.containsKey(key)) {
                OfferOutcome.Ignored
            } else {
                map[key] = msg
                order.add(msg)
                condition.signalAll()
                OfferOutcome.Created
            }
        }
    }

    override fun <In> offerBatch(
        messages: java.util.List<BatchedMessage<In, A>>
    ): java.util.List<BatchedReply<In, A>> {
        val replies = ArrayList<BatchedReply<In, A>>(messages.size)
        for (batched in messages) {
            val outcome =
                if (batched.message.canUpdate) {
                    offerOrUpdate(
                        batched.message.key,
                        batched.message.payload,
                        batched.message.scheduleAt,
                    )
                } else {
                    offerIfNotExists(
                        batched.message.key,
                        batched.message.payload,
                        batched.message.scheduleAt,
                    )
                }
            replies.add(BatchedReply(batched.input, batched.message, outcome))
        }
        return java.util.Collections.unmodifiableList(replies)
    }

    override fun tryPoll(): AckEnvelope<A>? {
        val now = clock.now()

        return lock.withLock {
            val first = order.firstOrNull()
            if (first != null && first.scheduleAt <= now) {
                // Remove from both collections
                order.remove(first)
                map.remove(first.key)

                // Reschedule for redelivery
                val expiresAt = now.plus(timeConfig.acquireTimeout)
                val rescheduled =
                    Message(
                        first.key,
                        first.payload,
                        expiresAt,
                        first.scheduledAtInitially,
                        first.createdAt,
                        ackEnvSource,
                        DeliveryType.REDELIVERY,
                    )
                map[first.key] = rescheduled
                order.add(rescheduled)

                // Return envelope with original delivery type
                AckEnvelope(
                    payload = first.payload,
                    messageId = MessageId(first.key),
                    timestamp = now,
                    source = first.ackEnvSource,
                    deliveryType = first.deliveryType,
                    acknowledgeHandler = AckHandler { acknowledgeMessage(first) },
                )
            } else {
                null
            }
        }
    }

    override fun tryPollMany(batchMaxSize: Int): AckEnvelope<java.util.List<A>> {
        val messages = ArrayList<A>()
        val acks = ArrayList<AckHandler>()
        var source = ackEnvSource
        var deliveryType = DeliveryType.FIRST_DELIVERY

        // Poll up to batchMaxSize messages
        for (i in 0 until batchMaxSize) {
            val envelope = tryPoll() ?: break
            messages.add(envelope.payload)
            acks.add(AckHandler { envelope.acknowledge() })
            source = envelope.source
            if (envelope.deliveryType == DeliveryType.REDELIVERY) {
                deliveryType = DeliveryType.REDELIVERY
            }
        }

        val messageId = MessageId(UUID.randomUUID().toString())
        val now = clock.now()

        return AckEnvelope(
            payload = java.util.Collections.unmodifiableList(messages),
            messageId = messageId,
            timestamp = now,
            source = source,
            deliveryType = deliveryType,
            acknowledgeHandler = AckHandler { acks.forEach { it.acknowledge() } },
        )
    }

    @Throws(InterruptedException::class)
    override fun poll(): AckEnvelope<A> {
        lock.withLock {
            while (true) {
                val envelope = tryPollUnlocked()
                if (envelope != null) {
                    return envelope
                }
                // Wait for a message or timeout
                condition.await(
                    timeConfig.pollPeriod.toNanos(),
                    java.util.concurrent.TimeUnit.NANOSECONDS,
                )
            }
        }
    }

    /** Internal tryPoll without acquiring the lock (caller must hold lock). */
    private fun tryPollUnlocked(): AckEnvelope<A>? {
        val now = clock.now()
        val first = order.firstOrNull()
        if (first != null && first.scheduleAt <= now) {
            order.remove(first)
            map.remove(first.key)

            // Reschedule for redelivery
            val expiresAt = now.plus(timeConfig.acquireTimeout)
            val rescheduled =
                Message(
                    first.key,
                    first.payload,
                    expiresAt,
                    first.scheduledAtInitially,
                    first.createdAt,
                    ackEnvSource,
                    DeliveryType.REDELIVERY,
                )
            map[first.key] = rescheduled
            order.add(rescheduled)

            return AckEnvelope(
                payload = first.payload,
                messageId = MessageId(first.key),
                timestamp = now,
                source = first.ackEnvSource,
                deliveryType = first.deliveryType,
                acknowledgeHandler = AckHandler { acknowledgeMessage(first) },
            )
        }
        return null
    }

    override fun read(key: String): AckEnvelope<A>? {
        val now = clock.now()
        return lock.withLock {
            val msg = map[key]
            if (msg != null) {
                AckEnvelope(
                    payload = msg.payload,
                    messageId = MessageId(msg.key),
                    timestamp = now,
                    source = msg.ackEnvSource,
                    deliveryType = msg.deliveryType,
                    acknowledgeHandler = AckHandler { acknowledgeMessage(msg) },
                )
            } else {
                null
            }
        }
    }

    override fun dropMessage(key: String): Boolean {
        return lock.withLock {
            val msg = map.remove(key)
            if (msg != null) {
                order.remove(msg)
                true
            } else {
                false
            }
        }
    }

    override fun containsMessage(key: String): Boolean {
        return lock.withLock { map.containsKey(key) }
    }

    override fun dropAllMessages(confirm: String): Int {
        require(confirm == "Yes, please, I know what I'm doing!") {
            "Incorrect confirmation string for dropAllMessages"
        }
        return lock.withLock {
            val count = map.size
            map.clear()
            order.clear()
            count
        }
    }

    override fun getCron(): CronService<A> {
        return cronService
    }

    private fun acknowledgeMessage(msg: Message<A>) {
        lock.withLock {
            // Only delete if the message hasn't been updated
            val current = map[msg.key]
            if (current != null && current.isSameData(msg)) {
                map.remove(msg.key)
                order.remove(current)
            }
        }
    }

    private fun deleteOldCron(keyPrefix: String) {
        lock.withLock {
            val toRemove = map.keys.filter { it.startsWith(keyPrefix) }
            for (key in toRemove) {
                val msg = map.remove(key)
                if (msg != null) {
                    order.remove(msg)
                }
            }
        }
    }

    private val cronService =
        object : CronService<A> {
            override fun installTick(
                configHash: ConfigHash,
                keyPrefix: String,
                messages: java.util.List<CronMessage<A>>,
            ) {
                deleteOldCron(keyPrefix)
                for (cronMsg in messages) {
                    val scheduledMsg = cronMsg.toScheduled(configHash, keyPrefix, canUpdate = false)
                    offerIfNotExists(
                        scheduledMsg.key,
                        scheduledMsg.payload,
                        scheduledMsg.scheduleAt,
                    )
                }
            }

            override fun uninstallTick(configHash: ConfigHash, keyPrefix: String) {
                deleteOldCron(keyPrefix)
            }

            override fun install(
                configHash: ConfigHash,
                keyPrefix: String,
                scheduleInterval: Duration,
                generateMany: CronMessageBatchGenerator<A>,
            ): AutoCloseable {
                return installLoop(
                    configHash = configHash,
                    keyPrefix = keyPrefix,
                    scheduleInterval = scheduleInterval,
                    generateMany = generateMany,
                )
            }

            override fun installDailySchedule(
                keyPrefix: String,
                schedule: DailyCronSchedule,
                generator: CronMessageGenerator<A>,
            ): AutoCloseable {
                return installLoop(
                    configHash = ConfigHash.fromDailyCron(schedule),
                    keyPrefix = keyPrefix,
                    scheduleInterval = schedule.scheduleInterval,
                    generateMany =
                        CronMessageBatchGenerator { now ->
                            val times = schedule.getNextTimes(now)
                            val batch = ArrayList<CronMessage<A>>(times.size)
                            for (time in times) {
                                batch.add(generator.generate(time))
                            }
                            java.util.Collections.unmodifiableList(batch)
                        },
                )
            }

            override fun installPeriodicTick(
                keyPrefix: String,
                period: Duration,
                generator: PayloadGenerator<A>,
            ): AutoCloseable {
                val scheduleInterval = Duration.ofSeconds(1).coerceAtLeast(period.dividedBy(4))
                return installLoop(
                    configHash = ConfigHash.fromPeriodicTick(period),
                    keyPrefix = keyPrefix,
                    scheduleInterval = scheduleInterval,
                    generateMany =
                        CronMessageBatchGenerator { now ->
                            val periodMillis = period.toMillis()
                            val timestamp =
                                Instant.ofEpochMilli(
                                    ((now.toEpochMilli() + periodMillis) / periodMillis) *
                                        periodMillis
                                )
                            java.util.List.of(
                                CronMessage(
                                    payload = generator.generate(timestamp),
                                    scheduleAt = timestamp,
                                )
                            )
                        },
                )
            }

            private fun installLoop(
                configHash: ConfigHash,
                keyPrefix: String,
                scheduleInterval: Duration,
                generateMany: CronMessageBatchGenerator<A>,
            ): AutoCloseable {
                val task =
                    org.funfix.tasks.jvm.Task.fromBlockingIO {
                        var isFirst = true
                        while (!Thread.interrupted()) {
                            try {
                                val now = clock.now()
                                val messages = generateMany.generate(now)
                                val canUpdate = isFirst
                                isFirst = false

                                deleteOldCron(keyPrefix)
                                for (cronMsg in messages) {
                                    val scheduledMsg =
                                        cronMsg.toScheduled(configHash, keyPrefix, canUpdate)
                                    if (canUpdate) {
                                        offerOrUpdate(
                                            scheduledMsg.key,
                                            scheduledMsg.payload,
                                            scheduledMsg.scheduleAt,
                                        )
                                    } else {
                                        offerIfNotExists(
                                            scheduledMsg.key,
                                            scheduledMsg.payload,
                                            scheduledMsg.scheduleAt,
                                        )
                                    }
                                }

                                Thread.sleep(scheduleInterval.toMillis())
                            } catch (e: InterruptedException) {
                                Thread.currentThread().interrupt()
                                break
                            }
                        }
                    }

                val fiber = task.runFiber()

                return AutoCloseable {
                    fiber.cancel()
                    fiber.joinBlockingUninterruptible()
                }
            }
        }

    /** Internal message representation with metadata. */
    private data class Message<A>(
        val key: String,
        val payload: A,
        val scheduleAt: Instant,
        val scheduledAtInitially: Instant,
        val createdAt: Instant,
        val ackEnvSource: String,
        val deliveryType: DeliveryType,
    ) {
        fun isSameData(other: Message<A>): Boolean {
            return key == other.key &&
                payload == other.payload &&
                scheduledAtInitially == other.scheduledAtInitially
        }

        companion object {
            fun <A> comparator(): Comparator<Message<A>> =
                compareBy<Message<A>> { it.scheduleAt }
                    .thenBy { it.key }
                    .thenBy { it.payload.hashCode() }
        }
    }

    public companion object {
        /**
         * Creates an in-memory delayed queue with default configuration.
         *
         * @param A the type of message payloads
         * @param timeConfig optional time configuration (defaults to 30s acquire timeout, 100ms
         *   poll period)
         * @param ackEnvSource optional source identifier for envelopes (defaults to
         *   "delayed-queue-inmemory")
         */
        @JvmStatic
        @JvmOverloads
        public fun <A> create(
            timeConfig: DelayedQueueTimeConfig =
                DelayedQueueTimeConfig(Duration.ofSeconds(30), Duration.ofMillis(100)),
            ackEnvSource: String = "delayed-queue-inmemory",
            clock: Clock = Clock.system(),
        ): DelayedQueueInMemory<A> {
            return DelayedQueueInMemory(timeConfig, ackEnvSource, clock)
        }
    }
}
