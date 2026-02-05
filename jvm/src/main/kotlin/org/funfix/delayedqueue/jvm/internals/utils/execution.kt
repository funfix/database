package org.funfix.delayedqueue.jvm.internals.utils

import java.util.concurrent.ExecutionException
import org.funfix.tasks.jvm.Task
import org.funfix.tasks.jvm.TaskExecutors

internal val DB_EXECUTOR by lazy { TaskExecutors.sharedBlockingIO() }

context(_: Raise<InterruptedException>)
internal fun <T> runBlockingIO(block: () -> T): T {
    val fiber = Task.fromBlockingIO { block() }.ensureRunningOnExecutor(DB_EXECUTOR).runFiber()

    try {
        return fiber.awaitBlocking()
    } catch (e: ExecutionException) {
        throw e.cause ?: e
    } catch (e: InterruptedException) {
        fiber.cancel()
        fiber.joinBlockingUninterruptible()
        throw e
    }
}

context(_: Raise<InterruptedException>)
internal fun <T> runBlockingIOUninterruptible(block: () -> T): T {
    val fiber = Task.fromBlockingIO { block() }.ensureRunningOnExecutor(DB_EXECUTOR).runFiber()

    fiber.joinBlockingUninterruptible()
    try {
        return fiber.resultOrThrow
    } catch (e: ExecutionException) {
        throw e.cause ?: e
    }
}
