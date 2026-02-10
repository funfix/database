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
import scala.concurrent.duration.FiniteDuration

/** Time configuration for delayed queue operations.
  *
  * @param acquireTimeout
  *   maximum time to wait when acquiring/locking a message for processing
  * @param pollPeriod
  *   interval between poll attempts when no messages are available
  */
final case class DelayedQueueTimeConfig(
  acquireTimeout: FiniteDuration,
  pollPeriod: FiniteDuration
) {

  /** Converts this Scala DelayedQueueTimeConfig to a JVM
    * DelayedQueueTimeConfig.
    */
  def asJava: jvm.DelayedQueueTimeConfig =
    new jvm.DelayedQueueTimeConfig(
      java.time.Duration.ofMillis(acquireTimeout.toMillis),
      java.time.Duration.ofMillis(pollPeriod.toMillis)
    )
}

object DelayedQueueTimeConfig {

  /** Default configuration for DelayedQueueInMemory. */
  val DEFAULT_IN_MEMORY: DelayedQueueTimeConfig =
    jvm.DelayedQueueTimeConfig.DEFAULT_IN_MEMORY.asScala

  /** Default configuration for JDBC-based implementations, with longer acquire
    * timeouts and poll periods to reduce database load in production
    * environments.
    */
  val DEFAULT_JDBC: DelayedQueueTimeConfig =
    jvm.DelayedQueueTimeConfig.DEFAULT_JDBC.asScala

  /** Default configuration for testing, with shorter timeouts and poll periods
    * to speed up tests.
    */
  val DEFAULT_TESTING: DelayedQueueTimeConfig =
    jvm.DelayedQueueTimeConfig.DEFAULT_TESTING.asScala

  /** Conversion extension for JVM DelayedQueueTimeConfig. */
  extension (javaConfig: jvm.DelayedQueueTimeConfig) {

    /** Converts a JVM DelayedQueueTimeConfig to a Scala DelayedQueueTimeConfig.
      */
    def asScala: DelayedQueueTimeConfig =
      DelayedQueueTimeConfig(
        acquireTimeout =
          FiniteDuration(
            javaConfig.acquireTimeout.toMillis,
            scala.concurrent.duration.MILLISECONDS
          ),
        pollPeriod =
          FiniteDuration(javaConfig.pollPeriod.toMillis, scala.concurrent.duration.MILLISECONDS)
      )
  }
}
