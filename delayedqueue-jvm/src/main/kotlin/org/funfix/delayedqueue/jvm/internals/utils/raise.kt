package org.funfix.delayedqueue.jvm.internals.utils

@JvmInline
internal value class Raise<in E : Exception> private constructor(val fake: Nothing? = null) {
    companion object {
        val _PRIVATE: Raise<Exception> = Raise()
    }
}

context(_: Raise<E>)
internal inline fun <reified E : Exception> raise(exception: E): Nothing = throw exception

internal inline fun <T> sneakyRaises(
    block:
        context(Raise<Exception>)
        () -> T
): T = block(Raise._PRIVATE)
