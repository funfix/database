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

package org.funfix.delayedqueue.scala

import java.security.MessageDigest
import org.funfix.delayedqueue.jvm

/** Hash of a cron configuration, used to detect configuration changes.
  *
  * When a cron schedule is installed, this hash is used to identify messages belonging to that
  * configuration. If the configuration changes, the hash will differ, allowing the system to clean
  * up old scheduled messages.
  *
  * @param value
  *   the MD5 hash string
  */
final case class CronConfigHash(value: String) {
  override def toString: String = value

  /** Converts this Scala CronConfigHash to a JVM CronConfigHash. */
  def asJava: jvm.CronConfigHash =
    new jvm.CronConfigHash(value)
}

object CronConfigHash {

  /** Creates a ConfigHash from a daily cron schedule configuration. */
  def fromDailyCron(config: CronDailySchedule): CronConfigHash = {
    // Port from Kotlin: buildString { appendLine(); appendLine("daily-cron:"); ... }
    val text = new StringBuilder()
    text.append('\n') // Leading newline to match Kotlin
    text.append("daily-cron:\n")
    text.append(s"  zone: ${config.zoneId}\n")
    text.append(s"  hours: ${config.hoursOfDay.mkString(", ")}\n")
    CronConfigHash(md5(text.toString))
  }

  /** Creates a ConfigHash from a periodic tick configuration. */
  def fromPeriodicTick(period: java.time.Duration): CronConfigHash = {
    // Port from Kotlin: buildString { appendLine(); appendLine("periodic-tick:"); ... }
    val text = new StringBuilder()
    text.append('\n') // Leading newline to match Kotlin
    text.append("periodic-tick:\n")
    text.append(s"  period-ms: ${period.toMillis}\n")
    CronConfigHash(md5(text.toString))
  }

  /** Creates a ConfigHash from an arbitrary string. */
  def fromString(text: String): CronConfigHash = CronConfigHash(md5(text))

  private def md5(input: String): String = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(input.getBytes)
    digest.map("%02x".format(_)).mkString
  }

  /** Conversion extension for JVM CronConfigHash. */
  extension (javaHash: jvm.CronConfigHash) {

    /** Converts a JVM CronConfigHash to a Scala CronConfigHash. */
    def asScala: CronConfigHash =
      CronConfigHash(javaHash.value)
  }
}
