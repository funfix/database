/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm.internals.utils

import java.util.concurrent.ExecutionException
import org.funfix.tasks.jvm.Task
import org.funfix.tasks.jvm.TaskExecutors

internal val DB_EXECUTOR by lazy { TaskExecutors.sharedBlockingIO() }

@Throws(InterruptedException::class)
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

@Throws(InterruptedException::class)
internal fun <T> runBlockingIOUninterruptible(block: () -> T): T {
    val fiber = Task.fromBlockingIO { block() }.ensureRunningOnExecutor(DB_EXECUTOR).runFiber()

    fiber.joinBlockingUninterruptible()
    try {
        return fiber.resultOrThrow
    } catch (e: ExecutionException) {
        throw e.cause ?: e
    }
}
