package org.funfix.delayedqueue.jvm.internals.utils

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import org.funfix.delayedqueue.jvm.RetryConfig

private inline fun <T> withRetryContext(
    block:
        context(Raise<InterruptedException>, Raise<java.util.concurrent.TimeoutException>)
        () -> T
): T =
    with(Raise.direct<InterruptedException>()) {
        with(Raise.direct<java.util.concurrent.TimeoutException>()) { block() }
    }

class RetryTests :
    FunSpec({
        context("RetryConfig") {
            test("should validate backoffFactor >= 1.0") {
                shouldThrow<IllegalArgumentException> {
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

            test("should validate non-negative delays") {
                shouldThrow<IllegalArgumentException> {
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

            test("should calculate exponential backoff correctly") {
                val config =
                    RetryConfig(
                        maxRetries = 5,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(10),
                        maxDelay = Duration.ofMillis(100),
                        backoffFactor = 2.0,
                    )

                val state0 = config.start(java.time.Instant.now())
                state0.delay shouldBe Duration.ofMillis(10)

                val state1 = state0.evolve(RuntimeException())
                state1.delay shouldBe Duration.ofMillis(20)

                val state2 = state1.evolve(RuntimeException())
                state2.delay shouldBe Duration.ofMillis(40)

                val state3 = state2.evolve(RuntimeException())
                state3.delay shouldBe Duration.ofMillis(80)

                val state4 = state3.evolve(RuntimeException())
                state4.delay shouldBe Duration.ofMillis(100) // capped at maxDelay
            }

            test("should track retries remaining") {
                val config =
                    RetryConfig(
                        maxRetries = 3,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(10),
                        maxDelay = Duration.ofMillis(100),
                        backoffFactor = 2.0,
                    )

                val state0 = config.start(java.time.Instant.now())
                state0.retriesRemaining shouldBe 3

                val state1 = state0.evolve(RuntimeException())
                state1.retriesRemaining shouldBe 2

                val state2 = state1.evolve(RuntimeException())
                state2.retriesRemaining shouldBe 1

                val state3 = state2.evolve(RuntimeException())
                state3.retriesRemaining shouldBe 0
            }

            test("should accumulate exceptions") {
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

                val state0 = config.start(java.time.Instant.now())
                val state1 = state0.evolve(ex1)
                val state2 = state1.evolve(ex2)
                val state3 = state2.evolve(ex3)

                state3.thrownExceptions shouldBe listOf(ex3, ex2, ex1)
            }

            test("prepareException should add suppressed exceptions") {
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

                val state =
                    config.start(java.time.Instant.now()).evolve(ex1).evolve(ex2).evolve(ex3)

                val prepared = state.prepareException(finalEx)
                prepared shouldBe finalEx
                prepared.suppressed shouldHaveSize 3
                prepared.suppressed[0] shouldBe ex3
                prepared.suppressed[1] shouldBe ex2
                prepared.suppressed[2] shouldBe ex1
            }
        }

        context("withRetries") {
            test("should succeed without retries if block succeeds") {
                val counter = AtomicInteger(0)
                val config =
                    RetryConfig(
                        maxRetries = 3,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(1),
                        maxDelay = Duration.ofMillis(10),
                        backoffFactor = 2.0,
                    )

                withRetryContext {
                    val result =
                        withRetries(config, { RetryOutcome.RETRY }) {
                            counter.incrementAndGet()
                            "success"
                        }

                    result shouldBe "success"
                    counter.get() shouldBe 1
                }
            }

            test("should retry on transient failures and eventually succeed") {
                val counter = AtomicInteger(0)
                val config =
                    RetryConfig(
                        maxRetries = 5,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(1),
                        maxDelay = Duration.ofMillis(10),
                        backoffFactor = 2.0,
                    )

                withRetryContext {
                    val result =
                        withRetries(config, { RetryOutcome.RETRY }) {
                            val count = counter.incrementAndGet()
                            if (count < 3) {
                                throw RuntimeException("transient failure")
                            }
                            "success"
                        }

                    result shouldBe "success"
                    counter.get() shouldBe 3
                }
            }

            test("should stop retrying when shouldRetry returns RAISE") {
                val counter = AtomicInteger(0)
                val config =
                    RetryConfig(
                        maxRetries = 5,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(1),
                        maxDelay = Duration.ofMillis(10),
                        backoffFactor = 2.0,
                    )

                withRetryContext {
                    val exception =
                        shouldThrow<ResourceUnavailableException> {
                            withRetries(config, { RetryOutcome.RAISE }) {
                                counter.incrementAndGet()
                                throw RuntimeException("permanent failure")
                            }
                        }

                    counter.get() shouldBe 1
                    exception.message shouldContain "Giving up after 0 retries"
                    exception.cause.shouldBeInstanceOf<RuntimeException>()
                    exception.cause?.message shouldBe "permanent failure"
                }
            }

            test("should exhaust maxRetries and fail") {
                val counter = AtomicInteger(0)
                val config =
                    RetryConfig(
                        maxRetries = 3,
                        totalSoftTimeout = null,
                        perTryHardTimeout = null,
                        initialDelay = Duration.ofMillis(1),
                        maxDelay = Duration.ofMillis(10),
                        backoffFactor = 2.0,
                    )

                withRetryContext {
                    val exception =
                        shouldThrow<ResourceUnavailableException> {
                            withRetries(config, { RetryOutcome.RETRY }) {
                                counter.incrementAndGet()
                                throw RuntimeException("always fails")
                            }
                        }

                    counter.get() shouldBe 4 // initial + 3 retries
                    exception.message shouldContain "Giving up after 3 retries"
                    exception.cause.shouldBeInstanceOf<RuntimeException>()
                    exception.cause?.suppressed shouldHaveSize 3
                }
            }

            test("should respect exponential backoff delays") {
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
                    )

                withRetryContext {
                    shouldThrow<ResourceUnavailableException> {
                        withRetries(config, { RetryOutcome.RETRY }) {
                            timestamps.add(System.currentTimeMillis())
                            counter.incrementAndGet()
                            throw RuntimeException("always fails")
                        }
                    }

                    timestamps shouldHaveSize 4
                    val delay1 = timestamps[1] - timestamps[0]
                    val delay2 = timestamps[2] - timestamps[1]
                    val delay3 = timestamps[3] - timestamps[2]

                    delay1 shouldBeGreaterThanOrEqual 40L // ~50ms with some tolerance
                    delay1 shouldBeLessThan 150L

                    delay2 shouldBeGreaterThanOrEqual 90L // ~100ms
                    delay2 shouldBeLessThan 250L

                    delay3 shouldBeGreaterThanOrEqual 190L // ~200ms (capped)
                    delay3 shouldBeLessThan 350L
                }
            }

            test("should handle per-try timeout") {
                val counter = AtomicInteger(0)
                val config =
                    RetryConfig(
                        maxRetries = 2,
                        totalSoftTimeout = null,
                        perTryHardTimeout = Duration.ofMillis(100),
                        initialDelay = Duration.ofMillis(1),
                        maxDelay = Duration.ofMillis(10),
                        backoffFactor = 2.0,
                    )

                withRetryContext {
                    val exception =
                        shouldThrow<RequestTimeoutException> {
                            withRetries(config, { RetryOutcome.RETRY }) {
                                counter.incrementAndGet()
                                Thread.sleep(500)
                                "should not reach here"
                            }
                        }

                    counter.get() shouldBeGreaterThanOrEqual 1
                    exception.message shouldContain "Giving up"
                }
            }
        }
    })
