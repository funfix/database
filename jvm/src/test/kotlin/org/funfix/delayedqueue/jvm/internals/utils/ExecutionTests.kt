package org.funfix.delayedqueue.jvm.internals.utils

import java.util.concurrent.ExecutionException
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
    fun `runBlockingIO propagates InterruptedException`() {
        val interrupted = InterruptedException("interrupted")
        val thrown =
            assertThrows(InterruptedException::class.java) {
                sneakyRaises { runBlockingIO { throw interrupted } }
            }
        assertEquals(interrupted, thrown)
    }

    @Test
    fun `runBlockingIO runs on shared executor`() = sneakyRaises {
        val threadName = runBlockingIO { Thread.currentThread().name }
        assertTrue(threadName.contains("pool"))
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
    fun `runBlockingIOUninterruptible does not propagate InterruptedException`() = sneakyRaises {
        val interrupted = InterruptedException("interrupted")
        // Should not throw InterruptedException, but wrap it
        val thrown =
            assertThrows(ExecutionException::class.java) {
                runBlockingIOUninterruptible { throw interrupted }
            }
        assertTrue(thrown.cause is InterruptedException)
    }
}
