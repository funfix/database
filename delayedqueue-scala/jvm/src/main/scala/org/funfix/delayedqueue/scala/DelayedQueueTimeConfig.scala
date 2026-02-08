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

import java.time.Duration
import org.funfix.delayedqueue.jvm

/** Time configuration for delayed queue operations.
  *
  * @param acquireTimeout
  *   maximum time to wait when acquiring/locking a message for processing
  * @param pollPeriod
  *   interval between poll attempts when no messages are available
  */
final case class DelayedQueueTimeConfig(
    acquireTimeout: Duration,
    pollPeriod: Duration
) {

  /** Converts this Scala DelayedQueueTimeConfig to a JVM DelayedQueueTimeConfig. */
  def asJava: jvm.DelayedQueueTimeConfig =
    new jvm.DelayedQueueTimeConfig(acquireTimeout, pollPeriod)
}

object DelayedQueueTimeConfig {

  /** Default configuration for DelayedQueueInMemory. */
  val DEFAULT_IN_MEMORY: DelayedQueueTimeConfig =
    DelayedQueueTimeConfig(
      acquireTimeout = Duration.ofMinutes(5),
      pollPeriod = Duration.ofMillis(500)
    )

  /** Default configuration for JDBC-based implementations, with longer acquire timeouts and poll
    * periods to reduce database load in production environments.
    */
  val DEFAULT_JDBC: DelayedQueueTimeConfig =
    DelayedQueueTimeConfig(
      acquireTimeout = Duration.ofMinutes(5),
      pollPeriod = Duration.ofSeconds(3)
    )

  /** Default configuration for testing, with shorter timeouts and poll periods to speed up tests.
    */
  val DEFAULT_TESTING: DelayedQueueTimeConfig =
    DelayedQueueTimeConfig(
      acquireTimeout = Duration.ofSeconds(30),
      pollPeriod = Duration.ofMillis(100)
    )

  /** Conversion extension for JVM DelayedQueueTimeConfig. */
  extension (javaConfig: jvm.DelayedQueueTimeConfig) {

    /** Converts a JVM DelayedQueueTimeConfig to a Scala DelayedQueueTimeConfig. */
    def asScala: DelayedQueueTimeConfig =
      DelayedQueueTimeConfig(
        acquireTimeout = javaConfig.acquireTimeout,
        pollPeriod = javaConfig.pollPeriod
      )
  }
}
