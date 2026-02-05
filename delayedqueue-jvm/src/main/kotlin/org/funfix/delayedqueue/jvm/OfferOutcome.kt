package org.funfix.delayedqueue.jvm

/**
 * Outcome of offering a message to the delayed queue.
 *
 * This sealed interface represents the possible results when adding or updating a message in the
 * queue.
 */
public sealed interface OfferOutcome {
    /** Returns true if the offer was ignored (message already exists and cannot be updated). */
    public fun isIgnored(): Boolean = this is Ignored

    /** Message was successfully created (new entry). */
    public data object Created : OfferOutcome

    /** Message was successfully updated (existing entry modified). */
    public data object Updated : OfferOutcome

    /** Message offer was ignored (already exists and canUpdate was false). */
    public data object Ignored : OfferOutcome
}
