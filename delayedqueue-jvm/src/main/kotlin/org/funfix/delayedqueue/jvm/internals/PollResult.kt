package org.funfix.delayedqueue.jvm.internals

import org.funfix.delayedqueue.jvm.AckEnvelope

/**
 * Internal sealed class representing the outcome of attempting to acquire a message. Used to avoid
 * fake sentinel values in the control flow.
 */
internal sealed interface PollResult<out A> {
    /** No messages are available in the queue. */
    data object NoMessages : PollResult<Nothing>

    /** Failed to acquire a message due to concurrent modification; caller should retry. */
    data object Retry : PollResult<Nothing>

    /** Successfully acquired a message. */
    data class Success<A>(val envelope: AckEnvelope<A>) : PollResult<A>
}
