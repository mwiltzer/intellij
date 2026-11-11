package io.confluent.intellijplugin.util

import com.intellij.testFramework.junit5.TestApplication
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CountDownLatch
import javax.swing.SwingUtilities
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TestTimeSource

// A UI thread that is held up must never hold up the build with it.
@Timeout(30)
@TestApplication
class LogTimingOnEdtTest {

    private val logger = RecordingLogger()
    private val clock = TestTimeSource()

    // Holds the UI thread until it is released, so that work for the UI thread has to wait.
    private class BusyEdt {
        private val release = CountDownLatch(1)

        init {
            val busy = CountDownLatch(1)
            SwingUtilities.invokeLater {
                busy.countDown()
                release.await()
            }
            busy.await()
        }

        fun release() = release.countDown()
    }

    @Test
    fun `should run the work on the UI thread and return what it gives`() = runBlocking<Unit> {
        val onEdt = logger.timedOnEdt("Showing", timeSource = clock) { SwingUtilities.isEventDispatchThread() }

        assertTrue(onEdt)
    }

    @Test
    fun `should log at debug that the work was quick, and that it did not wait`() = runBlocking<Unit> {
        val result = logger.timedOnEdt("Showing", timeSource = clock) {
            clock += 40.milliseconds
            "shown"
        }

        assertEquals("shown", result)
        assertEquals(listOf("debug: Showing took 40 ms, 0 ms of it waiting for the UI thread"), logger.lines)
    }

    @Test
    fun `should tell how long the work waited for the UI thread`() = runBlocking<Unit> {
        val busy = BusyEdt()
        val shown = async(Dispatchers.Default) {
            logger.timedOnEdt("Showing", timeSource = clock) { clock += 30.milliseconds }
        }
        // Real time for the work to ask for the UI thread, the clock of the log is the one of the test.
        Thread.sleep(200)
        clock += 1.2.seconds
        busy.release()
        shown.await()

        assertEquals(listOf("info: Showing took 1230 ms, 1200 ms of it waiting for the UI thread"), logger.lines)
    }

    @Test
    fun `should use the given threshold`() = runBlocking<Unit> {
        logger.timedOnEdt("Showing", slowAfterMillis = 500, timeSource = clock) { clock += 600.milliseconds }

        assertEquals(listOf("info: Showing took 600 ms, 0 ms of it waiting for the UI thread"), logger.lines)
    }

    @Test
    fun `should log that the work stopped, and pass the exception on`() = runBlocking<Unit> {
        val thrown = assertThrows<IllegalStateException> {
            logger.timedOnEdt("Showing", timeSource = clock) {
                clock += 5.milliseconds
                throw IllegalStateException("combo box gone")
            }
        }

        // The coroutines may hand it on as a copy, which tells the same.
        assertEquals("combo box gone", thrown.message)
        assertEquals(listOf("debug: Showing stopped after 5 ms, 0 ms of it waiting for the UI thread"), logger.lines)
    }

    @Test
    fun `should log that the UI thread never took the work when it is cancelled while it waits`() = runBlocking<Unit> {
        val busy = BusyEdt()
        val shown = async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) {
            logger.timedOnEdt("Showing", timeSource = clock) { "never" }
        }
        clock += 300.milliseconds

        // The cancellation is only noticed when the UI thread gets to the work, so it has to be free for that.
        shown.cancel()
        busy.release()
        shown.join()

        assertTrue(shown.isCancelled)
        assertThrows<CancellationException> { shown.await() }
        assertEquals(listOf("debug: Showing stopped after 300 ms, before the UI thread took it"), logger.lines)
    }
}
