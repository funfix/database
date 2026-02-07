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

import java.security.MessageDigest
import java.time.Duration

/**
 * Hash of a cron configuration, used to detect configuration changes.
 *
 * When a cron schedule is installed, this hash is used to identify messages belonging to that
 * configuration. If the configuration changes, the hash will differ, allowing the system to clean
 * up old scheduled messages.
 *
 * @property value the MD5 hash string
 */
@JvmRecord
public data class CronConfigHash(public val value: String) {
    override fun toString(): String = value

    public companion object {
        /** Creates a ConfigHash from a daily cron schedule configuration. */
        @JvmStatic
        public fun fromDailyCron(config: CronDailySchedule): CronConfigHash {
            val text = buildString {
                appendLine() // Leading newline to match Scala stripMargin
                appendLine("daily-cron:")
                appendLine("  zone: ${config.zoneId}")
                appendLine("  hours: ${config.hoursOfDay.joinToString(", ")}")
            }
            return CronConfigHash(md5(text))
        }

        /** Creates a ConfigHash from a periodic tick configuration. */
        @JvmStatic
        public fun fromPeriodicTick(period: Duration): CronConfigHash {
            val text = buildString {
                appendLine() // Leading newline to match Scala stripMargin
                appendLine("periodic-tick:")
                appendLine("  period-ms: ${period.toMillis()}")
            }
            return CronConfigHash(md5(text))
        }

        /** Creates a ConfigHash from an arbitrary string. */
        @JvmStatic public fun fromString(text: String): CronConfigHash = CronConfigHash(md5(text))

        private fun md5(input: String): String {
            val md = MessageDigest.getInstance("MD5")
            val digest = md.digest(input.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
