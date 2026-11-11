package io.confluent.intellijplugin.data

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class ExpiringSingleFlightTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val now = AtomicLong(1_000)
    private val loads = AtomicInteger()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private fun flightOf(ttlMillis: Long = 10_000, load: suspend (Int) -> String) =
        ExpiringSingleFlight(scope, ttlMillis, now::get) { load(loads.incrementAndGet()) }

    @Test
    fun `should load once for callers that ask at the same time`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val flight = flightOf { n -> release.await(); "value-$n" }

        val callers = (1..5).map { async(Dispatchers.Default) { flight.get() } }
        release.complete(Unit)

        assertEquals(List(5) { "value-1" }, withTimeout(5_000) { callers.awaitAll() })
        assertEquals(1, loads.get())
    }

    @Test
    fun `should serve the loaded value until it expires`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> "value-$n" }

        assertEquals("value-1", flight.get())
        now.addAndGet(99)
        assertEquals("value-1", flight.get())
        assertEquals(1, loads.get())

        now.addAndGet(1)
        assertEquals("value-2", flight.get())
        assertEquals(2, loads.get())
    }

    @Test
    fun `should load again after it was invalidated`() = runBlocking<Unit> {
        val flight = flightOf { n -> "value-$n" }
        assertEquals("value-1", flight.get())

        flight.invalidate()

        assertEquals("value-2", flight.get())
    }

    @Test
    fun `should not keep a value that was read before it was invalidated`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val flight = flightOf { n ->
            (if (n == 1) firstStarted else secondStarted).complete(Unit)
            release.await()
            "value-$n"
        }

        val beforeChange = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { firstStarted.await() }
        flight.invalidate()
        val afterChange = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { secondStarted.await() }
        release.complete(Unit)

        assertEquals("value-1", withTimeout(5_000) { beforeChange.await() })
        assertEquals("value-2", withTimeout(5_000) { afterChange.await() })
        // Only the load that started after the change is kept.
        assertEquals("value-2", flight.get())
        assertEquals(2, loads.get())
    }

    @Test
    fun `should share a failed load but not remember it`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val flight = flightOf { n ->
            release.await()
            if (n == 1) error("registry unavailable")
            "value-$n"
        }

        // Undispatched, so that all of them have joined the load before it is released.
        val callers = (1..3).map {
            async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { runCatching { flight.get() } }
        }
        release.complete(Unit)

        withTimeout(5_000) { callers.awaitAll() }.forEach {
            assertEquals("registry unavailable", it.exceptionOrNull()?.message)
        }
        assertEquals(1, loads.get())
        assertEquals("value-2", flight.get())
    }

    @Test
    fun `should keep loading for the others when a caller gives up`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val flight = flightOf { n -> started.complete(Unit); release.await(); "value-$n" }

        val quitter = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { started.await() }
        val stayer = async(Dispatchers.Default) { flight.get() }

        quitter.cancel()
        release.complete(Unit)

        assertEquals("value-1", withTimeout(5_000) { stayer.await() })
        assertEquals(1, loads.get())
    }

    @Test
    fun `should let a caller see the failure of its own load`() = runBlocking<Unit> {
        val flight = flightOf { error("boom") }

        assertThrows<IllegalStateException> { flight.get() }
    }

    @Test
    fun `should have nothing to peek at before the first load`() {
        val flight = flightOf { n -> "value-$n" }

        assertNull(flight.peek())
        assertEquals(0, loads.get())
    }

    @Test
    fun `should peek at the value that was loaded, also when it has expired`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> "value-$n" }
        flight.get()

        assertEquals("value-1", flight.peek())
        now.addAndGet(10_000)
        assertEquals("value-1", flight.peek())
        // Peeking is no reason to load.
        assertEquals(1, loads.get())
    }

    @Test
    fun `should peek at the new value once it was loaded again`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> "value-$n" }
        flight.get()
        now.addAndGet(100)

        assertEquals("value-2", flight.get())

        assertEquals("value-2", flight.peek())
    }

    @Test
    fun `should have nothing to peek at after it was invalidated`() = runBlocking<Unit> {
        val flight = flightOf { n -> "value-$n" }
        flight.get()

        flight.invalidate()

        assertNull(flight.peek())
    }

    @Test
    fun `should not peek at a value that was read before it was invalidated`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val flight = flightOf { n -> started.complete(Unit); release.await(); "value-$n" }

        val caller = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { started.await() }
        flight.invalidate()
        release.complete(Unit)

        assertEquals("value-1", withTimeout(5_000) { caller.await() })
        assertNull(flight.peek())
    }

    @Test
    fun `should not peek at a value of a load that failed`() = runBlocking<Unit> {
        val flight = flightOf { n -> if (n == 1) error("boom") else "value-$n" }

        assertThrows<IllegalStateException> { flight.get() }

        assertNull(flight.peek())
    }

    @Test
    fun `should keep the old value to peek at while a new load runs and fails`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> if (n == 1) "value-$n" else error("boom") }
        flight.get()
        now.addAndGet(100)

        assertThrows<IllegalStateException> { flight.get() }

        // A failed refresh says nothing against what was loaded before.
        assertEquals("value-1", flight.peek())
    }

    @Test
    fun `should have nothing fresh to peek at before the first load`() {
        val flight = flightOf { n -> "value-$n" }

        assertNull(flight.peekFresh())
        assertEquals(0, loads.get())
    }

    @Test
    fun `should peek at the fresh value until it expires`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> "value-$n" }
        flight.get()

        now.addAndGet(99)
        assertEquals("value-1", flight.peekFresh())

        now.addAndGet(1)
        assertNull(flight.peekFresh())
        // The value that has expired is still there for peek, and no peek loads.
        assertEquals("value-1", flight.peek())
        assertEquals(1, loads.get())
    }

    @Test
    fun `should have nothing fresh to peek at after it was invalidated`() = runBlocking<Unit> {
        val flight = flightOf { n -> "value-$n" }
        flight.get()

        flight.invalidate()

        assertNull(flight.peekFresh())
    }

    @Test
    fun `should peek at the new fresh value once it was loaded again`() = runBlocking<Unit> {
        val flight = flightOf(ttlMillis = 100) { n -> "value-$n" }
        flight.get()
        now.addAndGet(100)
        assertNull(flight.peekFresh())

        flight.get()

        assertEquals("value-2", flight.peekFresh())
    }

    @Test
    fun `should load again on refresh although the value has not expired`() = runBlocking<Unit> {
        val flight = flightOf { n -> "value-$n" }
        assertEquals("value-1", flight.get())

        assertEquals("value-2", flight.refresh())

        assertEquals(2, loads.get())
        // What a refresh loaded is what the next get serves.
        assertEquals("value-2", flight.get())
        assertEquals(2, loads.get())
    }

    @Test
    fun `should load on refresh when nothing was loaded yet`() = runBlocking<Unit> {
        val flight = flightOf { n -> "value-$n" }

        assertEquals("value-1", flight.refresh())

        assertEquals("value-1", flight.peek())
    }

    @Test
    fun `should join the load that runs when refreshing`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val flight = flightOf { n -> started.complete(Unit); release.await(); "value-$n" }

        val getter = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { started.await() }
        // Undispatched, so that all of them have joined the load before it is released.
        val refreshers = (1..3).map {
            async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { flight.refresh() }
        }
        release.complete(Unit)

        assertEquals("value-1", withTimeout(5_000) { getter.await() })
        assertEquals(List(3) { "value-1" }, withTimeout(5_000) { refreshers.awaitAll() })
        assertEquals(1, loads.get())
    }

    @Test
    fun `should share one load between callers that refresh at the same time`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val flight = flightOf { n -> release.await(); "value-$n" }

        // Undispatched, so that all of them have joined the load before it is released.
        val callers = (1..5).map {
            async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { flight.refresh() }
        }
        release.complete(Unit)

        assertEquals(List(5) { "value-1" }, withTimeout(5_000) { callers.awaitAll() })
        assertEquals(1, loads.get())
    }

    @Test
    fun `should still peek at the old value while a refresh runs`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val flight = flightOf { n ->
            if (n == 2) { started.complete(Unit); release.await() }
            "value-$n"
        }
        flight.get()

        val refresher = async(Dispatchers.Default) { flight.refresh() }
        withTimeout(5_000) { started.await() }

        assertEquals("value-1", flight.peek())
        release.complete(Unit)
        assertEquals("value-2", withTimeout(5_000) { refresher.await() })
        assertEquals("value-2", flight.peek())
    }

    @Test
    fun `should keep the old value when a refresh fails`() = runBlocking<Unit> {
        val flight = flightOf { n -> if (n == 2) error("boom") else "value-$n" }
        flight.get()

        assertThrows<IllegalStateException> { flight.refresh() }

        assertEquals("value-1", flight.peek())
        assertEquals("value-1", flight.get())
        assertEquals(2, loads.get())
    }

    @Test
    fun `should not join a load that was started before the value was invalidated`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val flight = flightOf { n ->
            (if (n == 1) firstStarted else secondStarted).complete(Unit)
            release.await()
            "value-$n"
        }

        val beforeChange = async(Dispatchers.Default) { flight.get() }
        withTimeout(5_000) { firstStarted.await() }
        flight.invalidate()
        val afterChange = async(Dispatchers.Default) { flight.refresh() }
        withTimeout(5_000) { secondStarted.await() }
        release.complete(Unit)

        assertEquals("value-1", withTimeout(5_000) { beforeChange.await() })
        assertEquals("value-2", withTimeout(5_000) { afterChange.await() })
        assertEquals("value-2", flight.peek())
    }

    private fun flightWithTtl(ttl: () -> Long) =
        ExpiringSingleFlight(scope, ttl, now::get) { "value-${loads.incrementAndGet()}" }

    @Test
    fun `should keep the value for as long as the time to live says when the value is read`() = runBlocking<Unit> {
        var ttl = 100L
        val flight = flightWithTtl { ttl }
        flight.get()
        now.addAndGet(150)
        assertNull(flight.peekFresh())

        // The value is no older, but it is allowed to be.
        ttl = 200
        assertEquals("value-1", flight.peekFresh())
        assertEquals("value-1", flight.get())
        assertEquals(1, loads.get())
    }

    @Test
    fun `should load again when the time to live got shorter than the age of the value`() = runBlocking<Unit> {
        var ttl = 1_000L
        val flight = flightWithTtl { ttl }
        flight.get()
        now.addAndGet(150)
        assertEquals("value-1", flight.get())

        ttl = 100

        assertNull(flight.peekFresh())
        assertEquals("value-2", flight.get())
        assertEquals(2, loads.get())
    }

    @Test
    fun `should take the time to live of the moment for a value that was loaded before it changed`() = runBlocking<Unit> {
        var ttl = 100L
        val flight = flightWithTtl { ttl }
        flight.get()
        ttl = 1_000

        now.addAndGet(999)
        assertEquals("value-1", flight.get())

        now.addAndGet(1)
        assertEquals("value-2", flight.get())
    }
}
