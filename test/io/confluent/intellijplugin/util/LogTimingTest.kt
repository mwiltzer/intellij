package io.confluent.intellijplugin.util

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TestTimeSource

class LogTimingTest {

    private val logger = RecordingLogger()
    private val clock = TestTimeSource()

    @Test
    fun `should log at debug and return the result when the work was quick`() {
        val result = logger.timed("Loading", timeSource = clock) {
            clock += 999.milliseconds
            "done"
        }

        assertEquals("done", result)
        assertEquals(listOf("debug: Loading took 999 ms"), logger.lines)
    }

    @Test
    fun `should log at info when the work took a second`() {
        logger.timed("Loading", timeSource = clock) { clock += 1.seconds }

        assertEquals(listOf("info: Loading took 1000 ms"), logger.lines)
    }

    @Test
    fun `should use the given threshold`() {
        logger.timed("Loading", slowAfterMillis = 50, timeSource = clock) { clock += 60.milliseconds }

        assertEquals(listOf("info: Loading took 60 ms"), logger.lines)
    }

    @Test
    fun `should log that the work stopped and pass the exception on`() {
        val failure = IllegalStateException("registry down")

        val thrown = assertThrows<IllegalStateException> {
            logger.timed("Loading", timeSource = clock) {
                clock += 2.seconds
                throw failure
            }
        }

        assertEquals(failure, thrown)
        assertEquals(listOf("info: Loading stopped after 2000 ms"), logger.lines)
    }

    @Test
    fun `should wrap suspending work and log a cancellation as stopped`() = runBlocking {
        logger.timed("Waiting", timeSource = clock) {
            delay(1)
            clock += 2.seconds
        }
        assertThrows<CancellationException> {
            logger.timed("Cancelled", timeSource = clock) {
                clock += 10.milliseconds
                delay(1)
                throw CancellationException("gone")
            }
        }

        assertEquals(listOf("info: Waiting took 2000 ms", "debug: Cancelled stopped after 10 ms"), logger.lines)
    }
}
