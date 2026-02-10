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

class RetryConfigSpec extends munit.FunSuite {

  test("DEFAULT should have correct values") {
    assertEquals(RetryConfig.DEFAULT.maxRetries, Some(5L))
    assertEquals(RetryConfig.DEFAULT.totalSoftTimeout, Some(Duration.ofSeconds(30)))
    assertEquals(RetryConfig.DEFAULT.perTryHardTimeout, Some(Duration.ofSeconds(10)))
    assertEquals(RetryConfig.DEFAULT.initialDelay, Duration.ofMillis(100))
    assertEquals(RetryConfig.DEFAULT.maxDelay, Duration.ofSeconds(5))
    assertEquals(RetryConfig.DEFAULT.backoffFactor, 2.0)
  }

  test("NO_RETRIES should have correct values") {
    assertEquals(RetryConfig.NO_RETRIES.maxRetries, Some(0L))
    assertEquals(RetryConfig.NO_RETRIES.totalSoftTimeout, None)
    assertEquals(RetryConfig.NO_RETRIES.perTryHardTimeout, None)
    assertEquals(RetryConfig.NO_RETRIES.backoffFactor, 1.0)
  }

  test("asJava and fromJava should be symmetric") {
    val original = RetryConfig.DEFAULT
    val roundTripped = RetryConfig.fromJava(original.asJava)

    assertEquals(roundTripped.maxRetries, original.maxRetries)
    assertEquals(roundTripped.totalSoftTimeout, original.totalSoftTimeout)
    assertEquals(roundTripped.perTryHardTimeout, original.perTryHardTimeout)
    assertEquals(roundTripped.initialDelay, original.initialDelay)
    assertEquals(roundTripped.maxDelay, original.maxDelay)
    assertEquals(roundTripped.backoffFactor, original.backoffFactor)
  }

  test("should validate backoffFactor >= 1.0") {
    intercept[IllegalArgumentException] {
      RetryConfig(
        initialDelay = Duration.ofMillis(100),
        maxDelay = Duration.ofSeconds(5),
        backoffFactor = 0.5,
        maxRetries = None,
        totalSoftTimeout = None,
        perTryHardTimeout = None
      )
    }
  }

  test("should validate non-negative delays") {
    intercept[IllegalArgumentException] {
      RetryConfig(
        initialDelay = Duration.ofMillis(-100),
        maxDelay = Duration.ofSeconds(5),
        backoffFactor = 2.0,
        maxRetries = None,
        totalSoftTimeout = None,
        perTryHardTimeout = None
      )
    }
  }
}
