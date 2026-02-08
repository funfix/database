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

package org.funfix.delayedqueue.jvm.internals.utils

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import org.funfix.delayedqueue.jvm.ResourceUnavailableException
import org.funfix.delayedqueue.jvm.RetryConfig
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RetryTests {

    @Nested
    inner class RetryConfigTest {
        @Test
        fun `should validate backoffFactor greater than or equal to 1_0`() {
            assertThrows(IllegalArgumentException::class.java) {
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 0.5,
                )
            }
        }

        @Test
        fun `should validate non-negative delays`() {
            assertThrows(IllegalArgumentException::class.java) {
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(-10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 2.0,
                )
            }
        }

        @Test
        fun `should calculate exponential backoff correctly`() {
            val config =
                RetryConfig(
                    maxRetries = 5,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 2.0,
                )

            val clock = java.time.Clock.systemUTC()
            val state0 = config.start(clock)
            assertEquals(Duration.ofMillis(10), state0.delay)

            val state1 = state0.evolve(RuntimeException())
            assertEquals(Duration.ofMillis(20), state1.delay)

            val state2 = state1.evolve(RuntimeException())
            assertEquals(Duration.ofMillis(40), state2.delay)

            val state3 = state2.evolve(RuntimeException())
            assertEquals(Duration.ofMillis(80), state3.delay)

            val state4 = state3.evolve(RuntimeException())
            assertEquals(Duration.ofMillis(100), state4.delay) // capped at maxDelay
        }

        @Test
        fun `should track retries remaining`() {
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 2.0,
                )

            val clock = java.time.Clock.systemUTC()
            val state0 = config.start(clock)
            assertEquals(3, state0.retriesRemaining)

            val state1 = state0.evolve(RuntimeException())
            assertEquals(2, state1.retriesRemaining)

            val state2 = state1.evolve(RuntimeException())
            assertEquals(1, state2.retriesRemaining)

            val state3 = state2.evolve(RuntimeException())
            assertEquals(0, state3.retriesRemaining)
        }

        @Test
        fun `should accumulate exceptions`() {
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 2.0,
                )

            val ex1 = RuntimeException("error 1")
            val ex2 = RuntimeException("error 2")
            val ex3 = RuntimeException("error 3")

            val clock = java.time.Clock.systemUTC()
            val state0 = config.start(clock)
            val state1 = state0.evolve(ex1)
            val state2 = state1.evolve(ex2)
            val state3 = state2.evolve(ex3)

            assertEquals(listOf(ex3, ex2, ex1), state3.thrownExceptions)
        }

        @Test
        fun `prepareException should add suppressed exceptions`() {
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ofMillis(100),
                    backoffFactor = 2.0,
                )

            val ex1 = RuntimeException("error 1")
            val ex2 = RuntimeException("error 2")
            val ex3 = RuntimeException("error 3")
            val finalEx = RuntimeException("final error")

            val clock = java.time.Clock.systemUTC()
            val state = config.start(clock).evolve(ex1).evolve(ex2).evolve(ex3)

            val prepared = state.prepareException(finalEx)
            assertEquals(finalEx, prepared)
            assertEquals(3, prepared.suppressed.size)
            assertEquals(ex3, prepared.suppressed[0])
            assertEquals(ex2, prepared.suppressed[1])
            assertEquals(ex1, prepared.suppressed[2])
        }
    }

    @Nested
    inner class WithRetriesTest {
        @Test
        fun `should succeed without retries if block succeeds`() {
            val counter = AtomicInteger(0)
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(1),
                    maxDelay = Duration.ofMillis(10),
                    backoffFactor = 2.0,
                ) {
                    val result =
                        withRetries(config, java.time.Clock.systemUTC(), { RetryOutcome.RETRY }) {
                            counter.incrementAndGet()
                            "success"
                        }

                    assertEquals("success", result)
                    assertEquals(1, counter.get())
                }
        }

        @Test
        fun `should retry on transient failures and eventually succeed`() {
            val counter = AtomicInteger(0)
            val config =
                RetryConfig(
                    maxRetries = 5,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(1),
                    maxDelay = Duration.ofMillis(10),
                    backoffFactor = 2.0,
                ) {
                    val result =
                        withRetries(config, java.time.Clock.systemUTC(), { RetryOutcome.RETRY }) {
                            val count = counter.incrementAndGet()
                            if (count < 3) {
                                throw RuntimeException("transient failure")
                            }
                            "success"
                        }

                    assertEquals("success", result)
                    assertEquals(3, counter.get())
                }
        }

        @Test
        fun `should stop retrying when shouldRetry returns RAISE`() {
            val counter = AtomicInteger(0)
            val config =
                RetryConfig(
                    maxRetries = 5,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(1),
                    maxDelay = Duration.ofMillis(10),
                    backoffFactor = 2.0,
                ) {
                    val exception =
                        assertThrows(ResourceUnavailableException::class.java) {
                            withRetries(
                                config,
                                java.time.Clock.systemUTC(),
                                { RetryOutcome.RAISE },
                            ) {
                                counter.incrementAndGet()
                                throw RuntimeException("permanent failure")
                            }
                        }

                    assertEquals(1, counter.get())
                    assertTrue(exception.message!!.contains("Giving up after 0 retries"))
                    assertInstanceOf(RuntimeException::class.java, exception.cause)
                    assertEquals("permanent failure", exception.cause?.message)
                }
        }

        @Test
        fun `should exhaust maxRetries and fail`() {
            val counter = AtomicInteger(0)
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(1),
                    maxDelay = Duration.ofMillis(10),
                    backoffFactor = 2.0,
                ) {
                    val exception =
                        assertThrows(ResourceUnavailableException::class.java) {
                            withRetries(
                                config,
                                java.time.Clock.systemUTC(),
                                { RetryOutcome.RETRY },
                            ) {
                                val attempt = counter.incrementAndGet()
                                throw RuntimeException("attempt $attempt failed")
                            }
                        }

                    assertEquals(4, counter.get()) // initial + 3 retries
                    assertTrue(exception.message!!.contains("Giving up after 3 retries"))
                    assertInstanceOf(RuntimeException::class.java, exception.cause)
                    assertEquals(3, exception.cause?.suppressed?.size)
                }
        }

        @Test
        fun `should respect exponential backoff delays`() {
            val counter = AtomicInteger(0)
            val timestamps = mutableListOf<Long>()
            val config =
                RetryConfig(
                    maxRetries = 3,
                    totalSoftTimeout = null,
                    perTryHardTimeout = null,
                    initialDelay = Duration.ofMillis(50),
                    maxDelay = Duration.ofMillis(200),
                    backoffFactor = 2.0,
                ) {
                    assertThrows(ResourceUnavailableException::class.java) {
                        withRetries(config, java.time.Clock.systemUTC(), { RetryOutcome.RETRY }) {
                            timestamps.add(System.currentTimeMillis())
                            counter.incrementAndGet()
                            throw RuntimeException("always fails")
                        }
                    }

                    assertEquals(4, timestamps.size)
                    val delay1 = timestamps[1] - timestamps[0]
                    val delay2 = timestamps[2] - timestamps[1]
                    val delay3 = timestamps[3] - timestamps[2]

                    assertTrue(delay1 >= 40L) // ~50ms with some tolerance
                    assertTrue(delay1 < 150L)

                    assertTrue(delay2 >= 90L) // ~100ms
                    assertTrue(delay2 < 250L)

                    assertTrue(delay3 >= 190L) // ~200ms (capped)
                    assertTrue(delay3 < 350L)
                }
        }

        @Test
        fun `should handle per-try timeout`() {
            val counter = AtomicInteger(0)
            val config =
                RetryConfig(
                    maxRetries = 2,
                    totalSoftTimeout = null,
                    perTryHardTimeout = Duration.ofMillis(100),
                    initialDelay = Duration.ofMillis(1),
                    maxDelay = Duration.ofMillis(10),
                    backoffFactor = 2.0,
                ) {
                    val exception =
                        assertThrows(java.util.concurrent.TimeoutException::class.java) {
                            withRetries(
                                config,
                                java.time.Clock.systemUTC(),
                                { RetryOutcome.RETRY },
                            ) {
                                counter.incrementAndGet()
                                Thread.sleep(500)
                            }
                        }

                    assertTrue(counter.get() >= 1)
                    assertTrue(exception.message!!.contains("Giving up"))
                }
        }
    }
}
