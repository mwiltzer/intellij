package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.SchemaUsage
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.cancellation.CancellationException

class RecordSchemaLookupTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val now = AtomicLong(1_000)
    private val asked = CopyOnWriteArrayList<Int>()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private fun usageOf(id: Int) = listOf(SchemaUsage("subject-$id", id))

    private fun lookupOf(
        capacity: Int = 100,
        fetch: suspend (Int) -> List<SchemaUsage>? = { usageOf(it) }
    ) = RecordSchemaLookup(
        scope = scope,
        ttlMillis = 1_000,
        failureTtlMillis = 100,
        capacity = capacity,
        clock = now::get
    ) { id ->
        asked += id
        fetch(id)
    }

    @Test
    fun `should ask the registry once for the records that share a schema`() = runBlocking<Unit> {
        val lookup = lookupOf()

        assertEquals(usageOf(7), lookup.usagesOf(7))
        assertEquals(usageOf(7), lookup.usagesOf(7))

        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should ask separately for each schema`() = runBlocking<Unit> {
        val lookup = lookupOf()

        assertEquals(usageOf(1), lookup.usagesOf(1))
        assertEquals(usageOf(2), lookup.usagesOf(2))

        assertEquals(listOf(1, 2), asked)
    }

    @Test
    fun `should ask once for callers that ask at the same time`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val lookup = lookupOf { release.await(); usageOf(it) }

        val callers = (1..5).map { async(Dispatchers.Default) { lookup.usagesOf(7) } }
        release.complete(Unit)

        assertEquals(List(5) { usageOf(7) }, withTimeout(5_000) { callers.awaitAll() })
        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should ask again once the answer is too old`() = runBlocking<Unit> {
        val lookup = lookupOf()
        lookup.usagesOf(7)

        now.addAndGet(999)
        lookup.usagesOf(7)
        assertEquals(listOf(7), asked)

        now.addAndGet(1)
        lookup.usagesOf(7)
        assertEquals(listOf(7, 7), asked)
    }

    @Test
    fun `should remember that the registry knows no schema with that ID`() = runBlocking<Unit> {
        val lookup = lookupOf { emptyList() }

        assertEquals(emptyList<SchemaUsage>(), lookup.usagesOf(7))
        assertEquals(emptyList<SchemaUsage>(), lookup.usagesOf(7))

        assertEquals(listOf(7), asked)
        // An empty answer is an answer: it is known, there is only nothing to show for it.
        assertEquals(emptyList<SchemaUsage>(), lookup.known(7))
    }

    @Test
    fun `should not ask again with every record while the registry fails, but later`() = runBlocking<Unit> {
        val lookup = lookupOf { null }

        assertNull(lookup.usagesOf(7))
        assertNull(lookup.usagesOf(7))
        assertEquals(listOf(7), asked)

        now.addAndGet(99)
        assertNull(lookup.usagesOf(7))
        assertEquals(listOf(7), asked)

        // A failure is not kept as long as an answer.
        now.addAndGet(1)
        assertNull(lookup.usagesOf(7))
        assertEquals(listOf(7, 7), asked)
    }

    @Test
    fun `should take a request that throws for a registry that could not be asked`() = runBlocking<Unit> {
        val lookup = lookupOf { throw IllegalStateException("registry is down") }

        assertNull(lookup.usagesOf(7))
        assertNull(lookup.usagesOf(7))

        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should get the answer after a failure`() = runBlocking<Unit> {
        val answers = ArrayDeque(listOf<List<SchemaUsage>?>(null, usageOf(7)))
        val lookup = lookupOf { answers.removeFirst() }

        assertNull(lookup.usagesOf(7))
        now.addAndGet(100)

        assertEquals(usageOf(7), lookup.usagesOf(7))
        assertEquals(usageOf(7), lookup.known(7))
    }

    @Test
    fun `should not stop the request of the others when a caller gives up`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val lookup = lookupOf { started.complete(Unit); release.await(); usageOf(it) }

        val quitter = async(Dispatchers.Default) { lookup.usagesOf(7) }
        val stayer = async(Dispatchers.Default) { lookup.usagesOf(7) }
        withTimeout(5_000) { started.await() }
        quitter.cancelAndJoin()
        release.complete(Unit)

        assertEquals(usageOf(7), withTimeout(5_000) { stayer.await() })
        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should keep the answer for later when the only caller gave up`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val lookup = lookupOf { started.complete(Unit); release.await(); usageOf(it) }

        val quitter = async(Dispatchers.Default) { lookup.usagesOf(7) }
        withTimeout(5_000) { started.await() }
        quitter.cancelAndJoin()
        release.complete(Unit)

        assertEquals(usageOf(7), withTimeout(5_000) { lookup.usagesOf(7) })
        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should not use a request that was cancelled for good`() = runBlocking<Unit> {
        val calls = AtomicInteger()
        val lookup = lookupOf {
            if (calls.incrementAndGet() == 1) throw CancellationException("cancelled")
            usageOf(it)
        }

        runCatching { lookup.usagesOf(7) }

        assertEquals(usageOf(7), withTimeout(5_000) { lookup.usagesOf(7) })
    }

    @Test
    fun `should know only what the registry told`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val lookup = lookupOf { release.await(); usageOf(it) }

        assertNull(lookup.known(7))

        val caller = async(Dispatchers.Default) { lookup.usagesOf(7) }
        // Asked, but not answered yet.
        assertNull(lookup.known(7))

        release.complete(Unit)
        withTimeout(5_000) { caller.await() }
        assertEquals(usageOf(7), lookup.known(7))
    }

    @Test
    fun `should not know an answer that is too old`() = runBlocking<Unit> {
        val lookup = lookupOf()
        lookup.usagesOf(7)

        now.addAndGet(1_000)

        assertNull(lookup.known(7))
    }

    @Test
    fun `should not know a failure`() = runBlocking<Unit> {
        val lookup = lookupOf { null }
        lookup.usagesOf(7)

        assertNull(lookup.known(7))
    }

    @Test
    fun `should forget everything when cleared`() = runBlocking<Unit> {
        val lookup = lookupOf()
        lookup.usagesOf(7)

        lookup.clear()

        assertNull(lookup.known(7))
        lookup.usagesOf(7)
        assertEquals(listOf(7, 7), asked)
    }

    @Test
    fun `should not keep what a request that was running when cleared brings`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val lookup = lookupOf { started.complete(Unit); release.await(); usageOf(it) }

        val beforeChange = async(Dispatchers.Default) { lookup.usagesOf(7) }
        withTimeout(5_000) { started.await() }
        lookup.clear()
        release.complete(Unit)

        assertEquals(usageOf(7), withTimeout(5_000) { beforeChange.await() })
        // It may have been read before the change, so nobody else gets it.
        assertNull(lookup.known(7))
    }

    @Test
    fun `should forget the schema that was asked for longest ago`() = runBlocking<Unit> {
        val lookup = lookupOf(capacity = 2)
        lookup.usagesOf(1)
        lookup.usagesOf(2)

        // Asking for 1 once more makes 2 the one asked for longest ago.
        lookup.usagesOf(1)
        lookup.usagesOf(3)

        assertEquals(usageOf(1), lookup.known(1))
        assertNull(lookup.known(2))
        assertEquals(usageOf(3), lookup.known(3))
    }
}
