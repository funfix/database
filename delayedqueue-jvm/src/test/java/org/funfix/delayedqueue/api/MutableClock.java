package org.funfix.delayedqueue.api;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

final class MutableClock extends Clock {
    private Instant instant;
    private final ZoneId zone;

    MutableClock(Instant initialInstant) {
        this(initialInstant, ZoneOffset.UTC);
    }

    MutableClock(Instant initialInstant, ZoneId zone) {
        this.instant = initialInstant;
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    Instant now() {
        return instant;
    }

    void setTime(Instant instant) {
        this.instant = instant;
    }

    void advance(Duration duration) {
        instant = instant.plus(duration);
    }
}
