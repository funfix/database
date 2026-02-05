package org.funfix.delayedqueue.jvm.internals

import java.sql.SQLException
import java.time.Clock
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.funfix.delayedqueue.jvm.CronConfigHash
import org.funfix.delayedqueue.jvm.CronDailySchedule
import org.funfix.delayedqueue.jvm.CronMessage
import org.funfix.delayedqueue.jvm.CronMessageBatchGenerator
import org.funfix.delayedqueue.jvm.CronMessageGenerator
import org.funfix.delayedqueue.jvm.CronPayloadGenerator
import org.funfix.delayedqueue.jvm.CronService
import org.funfix.delayedqueue.jvm.DelayedQueue
import org.slf4j.LoggerFactory

/**
 * Base implementation of CronService that can be used by both in-memory and JDBC implementations.
 */
internal class CronServiceImpl<A>(
    private val queue: DelayedQueue<A>,
    private val clock: Clock,
    private val deleteCurrentCron: (CronConfigHash, String) -> Unit,
    private val deleteOldCron: (CronConfigHash, String) -> Unit,
) : CronService<A> {
    private val logger = LoggerFactory.getLogger(CronServiceImpl::class.java)

    @Throws(SQLException::class, InterruptedException::class)
    override fun installTick(
        configHash: CronConfigHash,
        keyPrefix: String,
        messages: List<CronMessage<A>>,
    ) {
        installTick0(
            configHash = configHash,
            keyPrefix = keyPrefix,
            messages = messages,
            canUpdate = false,
        )
    }

    @Throws(SQLException::class, InterruptedException::class)
    override fun uninstallTick(configHash: CronConfigHash, keyPrefix: String) {
        deleteCurrentCron(configHash, keyPrefix)
    }

    @Throws(SQLException::class, InterruptedException::class)
    override fun install(
        configHash: CronConfigHash,
        keyPrefix: String,
        scheduleInterval: Duration,
        generateMany: CronMessageBatchGenerator<A>,
    ): AutoCloseable =
        install0(
            configHash = configHash,
            keyPrefix = keyPrefix,
            scheduleInterval = scheduleInterval,
            generateMany = generateMany,
        )

    @Throws(SQLException::class, InterruptedException::class)
    override fun installDailySchedule(
        keyPrefix: String,
        schedule: CronDailySchedule,
        generator: CronMessageGenerator<A>,
    ): AutoCloseable =
        install0(
            configHash = CronConfigHash.fromDailyCron(schedule),
            keyPrefix = keyPrefix,
            scheduleInterval = schedule.scheduleInterval,
            generateMany = { now ->
                schedule.getNextTimes(now).map { futureTime -> generator(futureTime) }
            },
        )

    @Throws(SQLException::class, InterruptedException::class)
    override fun installPeriodicTick(
        keyPrefix: String,
        period: Duration,
        generator: CronPayloadGenerator<A>,
    ): AutoCloseable {
        val configHash = CronConfigHash.fromString("periodic:$keyPrefix:${period.toMillis()}")
        return install0(
            configHash = configHash,
            keyPrefix = keyPrefix,
            scheduleInterval = period,
            generateMany = { now ->
                val next = now.plus(period)
                listOf(CronMessage(generator(next), next))
            },
        )
    }

    private fun installTick0(
        configHash: CronConfigHash,
        keyPrefix: String,
        messages: List<CronMessage<A>>,
        canUpdate: Boolean,
    ) {
        // Delete old messages from previous config
        deleteOldCron(configHash, keyPrefix)

        // Batch offer all messages
        val batchedMessages =
            messages.map { cronMessage ->
                org.funfix.delayedqueue.jvm.BatchedMessage(
                    input = Unit,
                    message =
                        cronMessage.toScheduled(
                            configHash = configHash,
                            keyPrefix = keyPrefix,
                            canUpdate = canUpdate,
                        ),
                )
            }

        if (batchedMessages.isNotEmpty()) {
            queue.offerBatch(batchedMessages)
        }
    }

    private fun install0(
        configHash: CronConfigHash,
        keyPrefix: String,
        scheduleInterval: Duration,
        generateMany: CronMessageBatchGenerator<A>,
    ): AutoCloseable {
        val executor: ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "cron-$keyPrefix").apply { isDaemon = true }
            }

        val isFirst = AtomicBoolean(true)

        val task = Runnable {
            try {
                val now = clock.instant()
                val firstRun = isFirst.getAndSet(false)
                val messages = generateMany(now)

                installTick0(
                    configHash = configHash,
                    keyPrefix = keyPrefix,
                    messages = messages,
                    canUpdate = firstRun,
                )
            } catch (e: Exception) {
                logger.error("Error in cron task for $keyPrefix", e)
            }
        }

        // Schedule with fixed delay, starting immediately
        executor.scheduleWithFixedDelay(task, 0, scheduleInterval.toMillis(), TimeUnit.MILLISECONDS)

        return AutoCloseable {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow()
                }
            } catch (e: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }
    }
}
