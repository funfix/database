package org.funfix.delayedqueue.jvm.internals.jdbc

import org.funfix.delayedqueue.jvm.ResourceUnavailableException
import org.funfix.delayedqueue.jvm.internals.utils.Raise
import org.funfix.delayedqueue.jvm.internals.utils.unsafeSneakyRaises

internal typealias PublicApiBlock<T> =
    context(Raise<InterruptedException>, Raise<ResourceUnavailableException>)
    () -> T

internal inline fun <T> publicApiThatThrows(block: PublicApiBlock<T>): T {
    return unsafeSneakyRaises { block() }
}
