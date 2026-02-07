/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm

import java.time.Duration
import java.time.Instant

/**
 * Service for installing cron-like periodic schedules in a delayed queue.
 *
 * This service allows installing tasks that execute at regular intervals or at specific times each
 * day. Configuration changes are detected via hash comparisons, allowing automatic cleanup of
 * obsolete schedules.
 *
 * @param A the type of message payload
 */
public interface CronService<A> {
    /**
     * Installs a one-time set of future scheduled messages.
     *
     * This method is useful for installing a fixed set of future events that belong to a specific
     * configuration (e.g., from a database or config file).
     *
     * The configHash and keyPrefix are used to identify and clean up messages when configurations
     * are updated or deleted.
     *
     * @param configHash hash identifying this configuration (for detecting changes)
     * @param keyPrefix prefix for all message keys in this configuration
     * @param messages list of messages to schedule
     * @throws ResourceUnavailableException if database operation fails after retries
     * @throws InterruptedException if the operation is interrupted
     */
    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    public fun installTick(
        configHash: CronConfigHash,
        keyPrefix: String,
        messages: List<CronMessage<A>>,
    )

    /**
     * Uninstalls all future messages for a specific cron configuration.
     *
     * This removes all scheduled messages that match the given configHash and keyPrefix.
     *
     * @param configHash hash identifying the configuration to remove
     * @param keyPrefix prefix for message keys to remove
     * @throws ResourceUnavailableException if database operation fails after retries
     * @throws InterruptedException if the operation is interrupted
     */
    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    public fun uninstallTick(configHash: CronConfigHash, keyPrefix: String)

    /**
     * Installs a cron-like schedule where messages are generated at intervals.
     *
     * This method starts a background process that periodically generates and schedules messages.
     * The returned AutoCloseable should be closed to stop the background process.
     *
     * When the configuration changes (detected by hash comparison), old messages are automatically
     * removed and new ones are scheduled.
     *
     * @param configHash hash of the configuration (for detecting changes)
     * @param keyPrefix unique prefix for generated message keys
     * @param scheduleInterval how often to regenerate/update the schedule
     * @param generateMany function that generates messages based on current time
     * @return an AutoCloseable resource that should be closed to stop scheduling
     * @throws ResourceUnavailableException if database operation fails after retries
     * @throws InterruptedException if the operation is interrupted
     */
    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    public fun install(
        configHash: CronConfigHash,
        keyPrefix: String,
        scheduleInterval: Duration,
        generateMany: CronMessageBatchGenerator<A>,
    ): AutoCloseable

    /**
     * Installs a daily schedule with timezone-aware execution times.
     *
     * This method starts a background process that schedules messages for specific hours each day.
     * The schedule configuration determines when messages are generated.
     *
     * @param keyPrefix unique prefix for generated message keys
     * @param schedule daily schedule configuration (hours, timezone, advance scheduling)
     * @param generator function that creates a message for a given future instant
     * @return an AutoCloseable resource that should be closed to stop scheduling
     * @throws ResourceUnavailableException if database operation fails after retries
     * @throws InterruptedException if the operation is interrupted
     */
    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    public fun installDailySchedule(
        keyPrefix: String,
        schedule: CronDailySchedule,
        generator: CronMessageGenerator<A>,
    ): AutoCloseable

    /**
     * Installs a periodic tick that generates messages at fixed intervals.
     *
     * This method starts a background process that generates a new message every `period` duration.
     * The generator receives the scheduled time and produces the payload.
     *
     * @param keyPrefix unique prefix for generated message keys
     * @param period interval between generated messages
     * @param generator function that creates a payload for a given instant
     * @return an AutoCloseable resource that should be closed to stop scheduling
     * @throws ResourceUnavailableException if database operation fails after retries
     * @throws InterruptedException if the operation is interrupted
     */
    @Throws(ResourceUnavailableException::class, InterruptedException::class)
    public fun installPeriodicTick(
        keyPrefix: String,
        period: Duration,
        generator: CronPayloadGenerator<A>,
    ): AutoCloseable
}

/** Generates a batch of cron messages based on the current instant. */
public fun interface CronMessageBatchGenerator<A> {
    /** Creates a batch of cron messages. */
    public operator fun invoke(now: Instant): List<CronMessage<A>>
}

/** Generates a single cron message for a given instant. */
public fun interface CronMessageGenerator<A> {
    /** Creates a cron message for the given instant. */
    public operator fun invoke(at: Instant): CronMessage<A>
}

/** Generates a payload for a given instant. */
public fun interface CronPayloadGenerator<A> {
    /** Creates a payload for the given instant. */
    public operator fun invoke(at: Instant): A
}
