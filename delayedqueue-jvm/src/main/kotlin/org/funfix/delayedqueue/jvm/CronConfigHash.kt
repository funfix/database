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
                appendLine("daily-cron:")
                appendLine("  zone: ${config.zoneId}")
                append("  hours: ${config.hoursOfDay.joinToString(", ")}")
            }
            return CronConfigHash(md5(text))
        }

        /** Creates a ConfigHash from a periodic tick configuration. */
        @JvmStatic
        public fun fromPeriodicTick(period: Duration): CronConfigHash {
            val text = buildString {
                appendLine("periodic-tick:")
                append("  period-ms: ${period.toMillis()}")
            }
            return CronConfigHash(md5(text))
        }

        private fun md5(input: String): String {
            val md = MessageDigest.getInstance("MD5")
            val digest = md.digest(input.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
