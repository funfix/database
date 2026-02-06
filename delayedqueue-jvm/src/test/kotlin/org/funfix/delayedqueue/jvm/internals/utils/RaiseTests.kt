package org.funfix.delayedqueue.jvm.internals.utils

import java.io.IOException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RaiseTests {
    @Test
    fun `sneakyRaises provides context receiver`() {
        val result = sneakyRaises { 123 }
        assertEquals(123, result)
    }

    @Test
    fun `raise throws exception in context`() {
        val thrown =
            assertThrows(IOException::class.java) { sneakyRaises { raise(IOException("fail")) } }
        assertEquals("fail", thrown.message)
    }

    @Test
    fun `sneakyRaises block can catch exception`() {
        val result =
            try {
                sneakyRaises { raise(IllegalArgumentException("bad")) }
                @Suppress("KotlinUnreachableCode") "no error"
            } catch (e: IllegalArgumentException) {
                e.message
            }
        assertEquals("bad", result)
    }

    @Test
    fun `Raise value class is internal and cannot be constructed externally`() {
        // This test is just to ensure the API is not public
        // Compilation will fail if you try: val r = Raise<Exception>()
        assertNotNull(Raise._PRIVATE_AND_UNSAFE)
    }
}
