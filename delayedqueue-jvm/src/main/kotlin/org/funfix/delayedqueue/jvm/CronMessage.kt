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

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * Represents a message for periodic (cron-like) scheduling.
 *
 * This wrapper is used for messages that should be scheduled repeatedly. The `scheduleAt` is used
 * to generate the unique key, while `scheduleAtActual` allows for a different execution time (e.g.,
 * to add a delay).
 *
 * @param A the type of the message payload
 * @property payload the message content
 * @property scheduleAt the nominal schedule time (used for key generation)
 * @property scheduleAtActual the actual execution time (defaults to scheduleAt if null)
 */
@JvmRecord
public data class CronMessage<out A>
@JvmOverloads
constructor(val payload: A, val scheduleAt: Instant, val scheduleAtActual: Instant? = null) {
    /**
     * Converts this CronMessage to a ScheduledMessage.
     *
     * @param configHash the configuration hash for this cron job
     * @param keyPrefix the prefix for generating unique keys
     * @param canUpdate whether the resulting message can update existing entries
     */
    public fun toScheduled(
        configHash: CronConfigHash,
        keyPrefix: String,
        canUpdate: Boolean,
    ): ScheduledMessage<A> =
        ScheduledMessage(
            key = key(configHash, keyPrefix, scheduleAt),
            payload = payload,
            scheduleAt = scheduleAtActual ?: scheduleAt,
            canUpdate = canUpdate,
        )

    public companion object {
        private val CRON_DATE_TIME_FORMATTER: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC)
        private const val NANOS_WIDTH = 9

        private fun formatTimestamp(scheduleAt: Instant): String {
            val nanos = String.format(Locale.ROOT, "%0${NANOS_WIDTH}d", scheduleAt.nano)
            return "${CRON_DATE_TIME_FORMATTER.format(scheduleAt)}.$nanos"
        }

        /**
         * Generates a unique key for a cron message.
         *
         * @param configHash the configuration hash
         * @param keyPrefix the key prefix
         * @param scheduleAt the schedule time
         * @return a unique key string
         */
        @JvmStatic
        public fun key(configHash: CronConfigHash, keyPrefix: String, scheduleAt: Instant): String =
            "$keyPrefix/${configHash.value}/${formatTimestamp(scheduleAt)}"

        /**
         * Creates a factory function that produces CronMessages with a static payload.
         *
         * @param payload the static payload to use for all generated messages
         * @return a function that creates CronMessages for any given instant
         */
        @JvmStatic
        public fun <A> staticPayload(payload: A): CronMessageGenerator<A> =
            CronMessageGenerator { scheduleAt ->
                CronMessage(payload, scheduleAt)
            }
    }
}
