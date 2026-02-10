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

/** Configuration for retry loops with exponential backoff.
  *
  * Used to configure retry behavior for database operations that may experience
  * transient failures such as deadlocks, connection issues, or transaction
  * rollbacks.
  *
  * ==Example==
  *
  * {{{
  * val config = RetryConfig(
  *   maxRetries = Some(3),
  *   totalSoftTimeout = Some(Duration.ofSeconds(30)),
  *   perTryHardTimeout = Some(Duration.ofSeconds(10)),
  *   initialDelay = Duration.ofMillis(100),
  *   maxDelay = Duration.ofSeconds(5),
  *   backoffFactor = 2.0
  * )
  * }}}
  *
  * @param initialDelay
  *   Initial delay before first retry
  * @param maxDelay
  *   Maximum delay between retries (backoff is capped at this value)
  * @param backoffFactor
  *   Multiplier for exponential backoff (e.g., 2.0 for doubling delays)
  * @param maxRetries
  *   Maximum number of retries (None means unlimited retries)
  * @param totalSoftTimeout
  *   Total time after which retries stop (None means no timeout)
  * @param perTryHardTimeout
  *   Hard timeout for each individual attempt (None means no per-try timeout)
  */
final case class RetryConfig(
  initialDelay: Duration,
  maxDelay: Duration,
  backoffFactor: Double,
  maxRetries: Option[Long],
  totalSoftTimeout: Option[Duration],
  perTryHardTimeout: Option[Duration]
) {
  require(backoffFactor >= 1.0, s"backoffFactor must be >= 1.0, got $backoffFactor")
  require(!initialDelay.isNegative, s"initialDelay must not be negative, got $initialDelay")
  require(!maxDelay.isNegative, s"maxDelay must not be negative, got $maxDelay")
  require(maxRetries.forall(_ >= 0), s"maxRetries must be >= 0 or None, got $maxRetries")
  require(
    totalSoftTimeout.forall(!_.isNegative),
    s"totalSoftTimeout must not be negative, got $totalSoftTimeout"
  )
  require(
    perTryHardTimeout.forall(!_.isNegative),
    s"perTryHardTimeout must not be negative, got $perTryHardTimeout"
  )

  /** Converts this Scala RetryConfig to a JVM RetryConfig. */
  def asJava: jvm.RetryConfig =
    new jvm.RetryConfig(
      initialDelay,
      maxDelay,
      backoffFactor,
      maxRetries.map(Long.box).getOrElse(null),
      totalSoftTimeout.getOrElse(null),
      perTryHardTimeout.getOrElse(null)
    )
}

object RetryConfig {

  /** Default retry configuration with reasonable defaults for database
    * operations:
    *   - 5 retries maximum
    *   - 30 second total timeout
    *   - 10 second per-try timeout
    *   - 100ms initial delay
    *   - 5 second max delay
    *   - 2.0 backoff factor (exponential doubling)
    */
  val DEFAULT: RetryConfig =
    fromJava(jvm.RetryConfig.DEFAULT)

  /** No retries - operations fail immediately on first error. */
  val NO_RETRIES: RetryConfig =
    fromJava(jvm.RetryConfig.NO_RETRIES)

  /** Converts a JVM RetryConfig to a Scala RetryConfig. */
  def fromJava(javaConfig: jvm.RetryConfig): RetryConfig =
    RetryConfig(
      initialDelay = javaConfig.initialDelay,
      maxDelay = javaConfig.maxDelay,
      backoffFactor = javaConfig.backoffFactor,
      maxRetries = Option(javaConfig.maxRetries).map(_.longValue),
      totalSoftTimeout = Option(javaConfig.totalSoftTimeout),
      perTryHardTimeout = Option(javaConfig.perTryHardTimeout)
    )
}
