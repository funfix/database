package org.funfix.delayedqueue.jvm

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference

/** A controllable clock for testing that allows manual time advancement. */
class TestClock(initialTime: Instant = Instant.EPOCH) : Clock() {
    private val current = AtomicReference(initialTime)

    override fun instant(): Instant = current.get()

    override fun getZone(): ZoneOffset = ZoneOffset.UTC

    override fun withZone(zone: java.time.ZoneId): Clock = this

    fun set(newTime: Instant) {
        current.set(newTime)
    }

    fun advance(duration: Duration) {
        current.updateAndGet { it.plus(duration) }
    }

    fun advanceSeconds(seconds: Long) = advance(Duration.ofSeconds(seconds))

    fun advanceMillis(millis: Long) = advance(Duration.ofMillis(millis))
}
