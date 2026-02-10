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

import org.funfix.delayedqueue.jvm

/** Hash of a cron configuration, used to detect configuration changes.
  *
  * When a cron schedule is installed, this hash is used to identify messages
  * belonging to that configuration. If the configuration changes, the hash will
  * differ, allowing the system to clean up old scheduled messages.
  */
final case class CronConfigHash(value: String) {
  /** Converts this Scala CronConfigHash to a JVM CronConfigHash. */
  def asJava: jvm.CronConfigHash =
    new jvm.CronConfigHash(value)
}

object CronConfigHash {
  /** Creates a ConfigHash from a daily cron schedule configuration. */
  def fromDailyCron(config: CronDailySchedule): CronConfigHash =
    fromJava(jvm.CronConfigHash.fromDailyCron(config.asJava))

  /** Creates a ConfigHash from a periodic tick configuration. */
  def fromPeriodicTick(period: java.time.Duration): CronConfigHash =
    fromJava(jvm.CronConfigHash.fromPeriodicTick(period))

  /** Creates a ConfigHash from an arbitrary string. */
  def fromString(text: String): CronConfigHash =
    fromJava(jvm.CronConfigHash.fromString(text))

  /** Converts a JVM CronConfigHash to a Scala CronConfigHash. */
  def fromJava(javaHash: jvm.CronConfigHash): CronConfigHash =
    CronConfigHash(javaHash.value)
}
