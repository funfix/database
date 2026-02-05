package org.funfix.delayedqueue.jvm.internals.utils

import java.util.concurrent.ExecutionException
import org.funfix.tasks.jvm.TaskCancellationException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ExecutionTests {
    @Test
    fun `runBlockingIO returns result`() = sneakyRaises {
        val result = runBlockingIO { 42 }
        assertEquals(42, result)
    }

    @Test
    fun `runBlockingIO propagates ExecutionException`() = sneakyRaises {
        val ex = ExecutionException("fail", null)
        val thrown = assertThrows(ExecutionException::class.java) { runBlockingIO { throw ex } }
        assertEquals(ex, thrown)
    }

    @Test
    fun `runBlockingIO propagates InterruptedException as TaskCancellationException`() {
        val interrupted = InterruptedException("interrupted")
        assertThrows(TaskCancellationException::class.java) {
            sneakyRaises { runBlockingIO { throw interrupted } }
        }
    }

    @Test
    fun `runBlockingIO runs on shared executor`() = sneakyRaises {
        val threadName = runBlockingIO { Thread.currentThread().name }
        assertTrue(threadName.contains("virtual"))
    }

    @Test
    fun `runBlockingIOUninterruptible returns result`() = sneakyRaises {
        val result = runBlockingIOUninterruptible { 99 }
        assertEquals(99, result)
    }

    @Test
    fun `runBlockingIOUninterruptible propagates ExecutionException`() = sneakyRaises {
        val ex = ExecutionException("fail", null)
        val thrown =
            assertThrows(ExecutionException::class.java) {
                runBlockingIOUninterruptible { throw ex }
            }
        assertEquals(ex, thrown)
    }

    @Test
    fun `runBlockingIOUninterruptible propagates InterruptedException as TaskCancellationException`() =
        sneakyRaises {
            val interrupted = InterruptedException("interrupted")
            // Should not throw InterruptedException, but wrap it
            assertThrows(TaskCancellationException::class.java) {
                runBlockingIOUninterruptible { throw interrupted }
            }
            Unit
        }
}
