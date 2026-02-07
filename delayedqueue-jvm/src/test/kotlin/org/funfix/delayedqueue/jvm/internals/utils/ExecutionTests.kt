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
import org.funfix.tasks.jvm.TaskCancellationException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.opentest4j.AssertionFailedError

class ExecutionTests {
    @Test
    fun `runBlockingIO returns result`() = unsafeSneakyRaises {
        val result = runBlockingIO { 42 }
        assertEquals(42, result)
    }

    @Test
    fun `runBlockingIO propagates ExecutionException`() = unsafeSneakyRaises {
        val ex = ExecutionException("fail", null)
        val thrown = assertThrows(ExecutionException::class.java) { runBlockingIO { throw ex } }
        assertEquals(ex, thrown)
    }

    @Test
    fun `runBlockingIO propagates InterruptedException as TaskCancellationException`() {
        val interrupted = InterruptedException("interrupted")
        assertThrows(TaskCancellationException::class.java) {
            unsafeSneakyRaises { runBlockingIO { throw interrupted } }
        }
    }

    @Test
    fun `runBlockingIO runs on shared executor`() = unsafeSneakyRaises {
        val threadName = runBlockingIO { Thread.currentThread().name }
        assertTrue(threadName.contains("virtual"))
    }

    @Test
    fun `runBlockingIO hangs when block throws AssertionFailedError`() {
        assertThrows(AssertionFailedError::class.java) {
            unsafeSneakyRaises { runBlockingIO { throw AssertionFailedError("boom") } }
        }
    }

    @Test
    fun `runBlockingIOUninterruptible returns result`() = unsafeSneakyRaises {
        val result = runBlockingIOUninterruptible { 99 }
        assertEquals(99, result)
    }

    @Test
    fun `runBlockingIOUninterruptible propagates ExecutionException`() = unsafeSneakyRaises {
        val ex = ExecutionException("fail", null)
        val thrown =
            assertThrows(ExecutionException::class.java) {
                runBlockingIOUninterruptible { throw ex }
            }
        assertEquals(ex, thrown)
    }

    @Test
    fun `runBlockingIOUninterruptible propagates InterruptedException as TaskCancellationException`() =
        unsafeSneakyRaises {
            val interrupted = InterruptedException("interrupted")
            // Should not throw InterruptedException, but wrap it
            assertThrows(TaskCancellationException::class.java) {
                runBlockingIOUninterruptible { throw interrupted }
            }
            Unit
        }
}
