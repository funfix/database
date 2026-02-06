package org.funfix.delayedqueue.jvm

/**
 * Checked exception thrown in case of exceptions happening that are not recoverable, rendering
 * DelayedQueue inaccessible.
 *
 * Example: issues with the RDBMS (bugs, or connection unavailable, failing after multiple retries)
 */
public class ResourceUnavailableException(message: String?, cause: Throwable?) :
    Exception(message, cause)
