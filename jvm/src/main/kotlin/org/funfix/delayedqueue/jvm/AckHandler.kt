package org.funfix.delayedqueue.jvm

/** Handles acknowledgment for a polled message. */
public fun interface AckHandler {
    /** Acknowledge successful processing. */
    public fun acknowledge()
}
