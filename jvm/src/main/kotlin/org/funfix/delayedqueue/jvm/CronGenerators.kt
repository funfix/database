package org.funfix.delayedqueue.jvm

import java.time.Instant
import java.util.List

/** Generates a batch of cron messages based on the current instant. */
public fun interface CronMessageBatchGenerator<A> {
    /** Creates a batch of cron messages. */
    public fun generate(now: Instant): List<CronMessage<A>>
}

/** Generates a single cron message for a given instant. */
public fun interface CronMessageGenerator<A> {
    /** Creates a cron message for the given instant. */
    public fun generate(at: Instant): CronMessage<A>
}

/** Generates a payload for a given instant. */
public fun interface PayloadGenerator<A> {
    /** Creates a payload for the given instant. */
    public fun generate(at: Instant): A
}
