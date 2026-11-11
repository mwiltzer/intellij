package io.confluent.intellijplugin.consumer.editor

import io.confluent.intellijplugin.data.SchemaUsageSource
import io.confluent.intellijplugin.registry.RecordSchema
import io.confluent.intellijplugin.registry.SchemaUsage
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class RecordSchemaDescriberTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val shown = LinkedBlockingQueue<String>()
    private val asked = CopyOnWriteArrayList<Int>()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private class FakeSource(
        private val cached: Map<Int, List<SchemaUsage>>,
        private val onAsk: suspend (Int) -> List<SchemaUsage>?
    ) : SchemaUsageSource {
        override suspend fun getSchemaUsages(schemaId: Int) = onAsk(schemaId)
        override fun getCachedSchemaUsages(schemaId: Int) = cached[schemaId]
    }

    private fun describerOf(source: SchemaUsageSource?) =
        RecordSchemaDescriber(scope, source, EmptyCoroutineContext) { shown.add(it) }

    private fun source(cached: Map<Int, List<SchemaUsage>> = emptyMap(), answer: suspend (Int) -> List<SchemaUsage>?) =
        FakeSource(cached) { id ->
            asked += id
            answer(id)
        }

    // What is shown at once, while the registry has not told yet.
    private fun shownAtOnce(): String? = shown.poll()

    // What is shown when the registry told.
    private fun shownLater(): String = shown.poll(5, TimeUnit.SECONDS) ?: error("nothing was shown")

    // Gives what could still be shown a chance to show up.
    private fun assertNothingMoreShown() {
        runBlocking { delay(150) }
        assertEquals(emptyList<String>(), shown.toList())
    }

    @Test
    fun `should show the ID at once and the subjects when the registry told`() {
        val release = CompletableDeferred<Unit>()
        val describer = describerOf(source { release.await(); listOf(SchemaUsage("orders-value", 3)) })

        describer.describe(RecordSchema(id = 100016))

        assertEquals("100016", shownAtOnce())
        release.complete(Unit)
        assertEquals("100016 (orders-value, version 3)", shownLater())
        assertNothingMoreShown()
    }

    @Test
    fun `should show what the registry told lately at once, without asking`() {
        val describer = describerOf(
            source(cached = mapOf(7 to listOf(SchemaUsage("orders-value", 3)))) { error("must not ask") }
        )

        describer.describe(RecordSchema(id = 7))

        assertEquals("7 (orders-value, version 3)", shownAtOnce())
        assertNothingMoreShown()
        assertEquals(emptyList<Int>(), asked)
    }

    @Test
    fun `should put the subjects of the topic first`() {
        val describer = describerOf(source { listOf(SchemaUsage("a-value", 1), SchemaUsage("orders-value", 3)) })

        describer.describe(RecordSchema(id = 7), topic = "orders")

        assertEquals("7", shownAtOnce())
        assertEquals("7 (orders-value, version 3; a-value, version 1)", shownLater())
    }

    @Test
    fun `should keep the ID when the registry does not know the schema`() {
        val describer = describerOf(source { emptyList() })

        describer.describe(RecordSchema(id = 7))

        assertEquals("7", shownAtOnce())
        assertNothingMoreShown()
        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should keep the ID when the registry could not be asked`() {
        val describer = describerOf(source { null })

        describer.describe(RecordSchema(id = 7))

        assertEquals("7", shownAtOnce())
        assertNothingMoreShown()
        assertEquals(listOf(7), asked)
    }

    @Test
    fun `should keep the ID when there is no registry to ask`() {
        val describer = describerOf(null)

        describer.describe(RecordSchema(id = 7))

        assertEquals("7", shownAtOnce())
        assertNothingMoreShown()
    }

    @Test
    fun `should show the GUID without asking for it`() {
        val guid = UUID.fromString("6c9d0a1e-1111-2222-3333-444455556666")
        val describer = describerOf(source { error("must not ask") })

        describer.describe(RecordSchema(guid = guid))

        assertEquals("GUID $guid", shownAtOnce())
        assertNothingMoreShown()
        assertEquals(emptyList<Int>(), asked)
    }

    @Test
    fun `should show nothing for a record that says nothing`() {
        val describer = describerOf(source { error("must not ask") })

        describer.describe(RecordSchema())
        assertEquals("", shownAtOnce())
        describer.describe(null)
        assertEquals("", shownAtOnce())

        assertNothingMoreShown()
        assertEquals(emptyList<Int>(), asked)
    }

    @Test
    fun `should not show what the registry told about the record that was selected before`() {
        val release = CompletableDeferred<Unit>()
        val describer = describerOf(source { id ->
            if (id == 1) {
                release.await()
                listOf(SchemaUsage("one-value", 1))
            } else {
                null
            }
        })

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        describer.describe(RecordSchema(id = 2))
        assertEquals("2", shownAtOnce())
        release.complete(Unit)

        assertNothingMoreShown()
    }

    @Test
    fun `should not show what the registry told once no record is selected`() {
        val release = CompletableDeferred<Unit>()
        val describer = describerOf(source { release.await(); listOf(SchemaUsage("one-value", 1)) })

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        describer.describe(null)
        assertEquals("", shownAtOnce())
        release.complete(Unit)

        assertNothingMoreShown()
    }

    @Test
    fun `should show what the registry told about the record that is selected now`() {
        val release = CompletableDeferred<Unit>()
        val describer = describerOf(source { id ->
            if (id == 1) release.await()
            listOf(SchemaUsage("subject-$id", id))
        })

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        describer.describe(RecordSchema(id = 2))
        assertEquals("2", shownAtOnce())

        assertEquals("2 (subject-2, version 2)", shownLater())
        release.complete(Unit)
        assertNothingMoreShown()
    }

    @Test
    fun `should stop asking for the record that was selected before`() {
        val release = CompletableDeferred<Unit>()
        val asking = CompletableDeferred<Unit>()
        val stopped = CompletableDeferred<Unit>()
        val describer = describerOf(source {
            asking.complete(Unit)
            try {
                release.await()
            } catch (e: CancellationException) {
                stopped.complete(Unit)
                throw e
            }
            listOf(SchemaUsage("one-value", 1))
        })

        describer.describe(RecordSchema(id = 1))
        runBlocking { withTimeout(5_000) { asking.await() } }
        describer.describe(RecordSchema(id = 2))

        runBlocking { withTimeout(5_000) { stopped.await() } }
    }

    @Test
    fun `should not show an answer that could not be stopped for a record that is not selected any more`() {
        val release = CompletableDeferred<Unit>()
        val asking = CompletableDeferred<Unit>()
        val describer = RecordSchemaDescriber(scope, source { id ->
            if (id == 1) {
                asking.complete(Unit)
                // A request that goes on whatever happens to the one who asked, like a blocked call of the registry client.
                suspendCoroutine<Unit> { request -> release.invokeOnCompletion { request.resume(Unit) } }
            }
            listOf(SchemaUsage("subject-$id", id))
        }, NonCancellable) { shown.add(it) }

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        runBlocking { withTimeout(5_000) { asking.await() } }
        describer.describe(RecordSchema(id = 2))
        assertEquals("2", shownAtOnce())
        assertEquals("2 (subject-2, version 2)", shownLater())
        release.complete(Unit)

        assertNothingMoreShown()
    }

    @Test
    fun `should show the subjects again when a record is selected once more`() {
        val describer = describerOf(source { id -> listOf(SchemaUsage("subject-$id", id)) })

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        assertEquals("1 (subject-1, version 1)", shownLater())

        describer.describe(RecordSchema(id = 1))
        assertEquals("1", shownAtOnce())
        assertEquals("1 (subject-1, version 1)", shownLater())
    }
}
