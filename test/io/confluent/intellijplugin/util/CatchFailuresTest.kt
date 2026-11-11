package io.confluent.intellijplugin.util

import com.intellij.openapi.progress.ProcessCanceledException
import kotlinx.coroutines.CancellationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail

class CatchFailuresTest {

    @Test
    fun `should return the result of the block`() {
        val result = catchFailures(onFailure = { "failed" }) { "done" }

        assertEquals("done", result)
    }

    @Test
    fun `should hand a failure over`() {
        val failure = IllegalStateException("boom")
        val seen = mutableListOf<Throwable>()

        val result = catchFailures(onFailure = { seen += it; "failed" }) { throw failure }

        assertEquals("failed", result)
        assertEquals(1, seen.size)
        assertSame(failure, seen.single())
    }

    @Test
    fun `should hand an error over, too`() {
        val failure = AssertionError("not an exception")

        val result = catchFailures(onFailure = { it }) { throw failure }

        assertSame(failure, result)
    }

    @Test
    fun `should throw a cancelled coroutine on`() {
        val cancelled = CancellationException("the job was cancelled")

        val thrown = assertThrows<CancellationException> {
            catchFailures<Unit>(onFailure = { fail("A cancellation is no failure, but got $it") }) { throw cancelled }
        }

        assertSame(cancelled, thrown)
    }

    @Test
    fun `should throw a cancelled process on`() {
        val cancelled = ProcessCanceledException()

        val thrown = assertThrows<ProcessCanceledException> {
            catchFailures<Unit>(onFailure = { fail("A cancellation is no failure, but got $it") }) { throw cancelled }
        }

        assertSame(cancelled, thrown)
    }
}
