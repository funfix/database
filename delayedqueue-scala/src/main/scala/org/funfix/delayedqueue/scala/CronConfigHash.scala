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
opaque type CronConfigHash = String

object CronConfigHash {

  /** Creates a CronConfigHash from a String value. */
  def apply(value: String): CronConfigHash = value

  /** Conversion extension for CronConfigHash. */
  extension (hash: CronConfigHash) {

    /** Gets the string value of the CronConfigHash. */
    def value: String = hash

    /** Converts this Scala CronConfigHash to a JVM CronConfigHash. */
    def asJava: jvm.CronConfigHash =
      new jvm.CronConfigHash(hash)
  }

  /** Creates a ConfigHash from a daily cron schedule configuration. */
  def fromDailyCron(config: CronDailySchedule): CronConfigHash =
    jvm.CronConfigHash.fromDailyCron(config.asJava).asScala

  /** Creates a ConfigHash from a periodic tick configuration. */
  def fromPeriodicTick(period: java.time.Duration): CronConfigHash =
    jvm.CronConfigHash.fromPeriodicTick(period).asScala

  /** Creates a ConfigHash from an arbitrary string. */
  def fromString(text: String): CronConfigHash =
    jvm.CronConfigHash.fromString(text).asScala

  /** Conversion extension for JVM CronConfigHash. */
  extension (javaHash: jvm.CronConfigHash) {

    /** Converts a JVM CronConfigHash to a Scala CronConfigHash. */
    def asScala: CronConfigHash =
      CronConfigHash(javaHash.value)
  }
}
