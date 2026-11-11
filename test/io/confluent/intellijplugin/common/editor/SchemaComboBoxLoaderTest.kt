package io.confluent.intellijplugin.common.editor

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.ComboBox
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.data.SchemaFormats
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import javax.swing.SwingUtilities

@TestApplication
class SchemaComboBoxLoaderTest {

    /**
     * Registry stand-in. The formats are only found out when the [resolveGate] is open, and the names only when the
     * [listingGate] is, if there is one. [cachedNames] are the names that were listed before.
     */
    private class FakeSource(
        @Volatile var names: List<String>?,
        private val formats: Map<String, KafkaRegistryFormat>
    ) : SchemaFormats {
        val resolveGate = CompletableDeferred<Unit>()

        @Volatile
        var listingGate: CompletableDeferred<Unit>? = null

        @Volatile
        var cachedNames: List<String>? = null
        val nameRequests = AtomicInteger()
        val formatRequests = ConcurrentLinkedQueue<String>()
        val resolveRequests = ConcurrentLinkedQueue<Pair<List<String>, List<String>>>()
        private val known = ConcurrentHashMap<String, KafkaRegistryFormat>()

        override suspend fun getSchemaNamesForEditor(): List<String>? {
            nameRequests.incrementAndGet()
            listingGate?.await()
            return names
        }

        override fun getCachedSchemaNames() = cachedNames

        override fun getKnownSchemaFormat(name: String) = known[name]

        override suspend fun getSchemaFormat(name: String): KafkaRegistryFormat? {
            formatRequests += name
            return formats[name]?.also { known[name] = it }
        }

        override suspend fun resolveSchemaFormats(
            names: Collection<String>,
            first: Collection<String>,
            onResolved: (name: String, format: KafkaRegistryFormat) -> Unit
        ) {
            resolveRequests += names.toList() to first.toList()
            resolveGate.await()
            (first + names).distinct().forEach { name ->
                val format = known[name] ?: formats[name]?.also { known[name] = it } ?: return@forEach
                onResolved(name, format)
            }
        }

        /** The names that were asked for in the requests of the loader, not counting the ones for the selection. */
        fun wantedBatches() = resolveRequests.filter { (_, first) -> first.isEmpty() }.map { it.first }

        fun urgentNames() = resolveRequests.flatMap { (_, first) -> first }.distinct()
    }

    private class CountingCombo : ComboBox<RegistrySchemaInEditor>(arrayOf<RegistrySchemaInEditor>()) {
        // Swing repaints while it builds the combo box, before the fields of this class exist.
        private var counter: AtomicInteger? = null
        val repaints = AtomicInteger().also { counter = it }

        override fun repaint() {
            counter?.incrementAndGet()
            super.repaint()
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Volatile
    private var topic: String? = null

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private val allFormats = mapOf(
        "orders-value" to KafkaRegistryFormat.PROTOBUF,
        "orders-key" to KafkaRegistryFormat.AVRO,
        "payments-value" to KafkaRegistryFormat.JSON,
        "audit-value" to KafkaRegistryFormat.AVRO,
        "new-value" to KafkaRegistryFormat.AVRO
    )

    private fun sourceOf(vararg names: String) = FakeSource(names.toList(), allFormats)

    private class Fixture(val combo: CountingCombo, val loader: SchemaComboBoxLoader)

    private fun fixtureFor(
        source: SchemaFormats,
        isKey: Boolean = false,
        reloadDelayMs: Long = 30,
        logger: Logger = Logger.getInstance(SchemaComboBoxLoader::class.java)
    ): Fixture {
        val combo = onEdt { CountingCombo() }
        val loader = SchemaComboBoxLoader(combo, source, scope, isKey, reloadDelayMs, logger) { topic }
        return Fixture(combo, loader)
    }

    private fun <T> onEdt(block: () -> T): T {
        var result: T? = null
        SwingUtilities.invokeAndWait { result = block() }
        @Suppress("UNCHECKED_CAST")
        return result as T
    }

    private fun eventually(what: String, timeoutMs: Long = 5_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (onEdt(condition)) return
            Thread.sleep(10)
        }
        fail<Unit>("Timed out waiting for $what")
    }

    private fun Fixture.names() = onEdt { (0 until combo.model.size).map { combo.model.getElementAt(it).schemaName } }

    private fun Fixture.items() = onEdt { (0 until combo.model.size).map { combo.model.getElementAt(it) } }

    private fun Fixture.item(name: String) = items().first { it.schemaName == name }

    private fun Fixture.selectedName() = onEdt { combo.item?.schemaName }

    private fun Fixture.format(name: String) = onEdt {
        (0 until combo.model.size).map { combo.model.getElementAt(it) }.first { it.schemaName == name }.schemaFormat
    }

    // For conditions: they run on the EDT already, and the schema may not be listed yet.
    private fun Fixture.formatNow(name: String) =
        (0 until combo.model.size).map { combo.model.getElementAt(it) }.firstOrNull { it.schemaName == name }?.schemaFormat

    private fun Fixture.select(name: String) = onEdt {
        combo.item = (0 until combo.model.size).map { combo.model.getElementAt(it) }.first { it.schemaName == name }
    }

    private fun Fixture.awaitNames(vararg expected: String) =
        eventually("the names ${expected.toList()}") {
            (0 until combo.model.size).map { combo.model.getElementAt(it).schemaName } == expected.toList()
        }

    // The list is shown again once it asks for formats with another selection than the one of "orders-value".
    private fun FakeSource.awaitListedAgain() =
        eventually("the list to be shown again") { resolveRequests.any { (_, first) -> first.firstOrNull() != "orders-value" } }

    @Nested
    inner class `listing` {

        @Test
        fun `should show the names before any format is known`() {
            val source = sourceOf("payments-value", "audit-value", "orders-value")
            val fixture = fixtureFor(source)

            fixture.loader.load()

            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            assertNull(fixture.format("audit-value"))
            assertNull(fixture.format("payments-value"))
            assertFalse(fixture.loader.isListing)
        }

        @Test
        fun `should say that it is listing until the names are there`() {
            val source = sourceOf("audit-value")
            val fixture = fixtureFor(source)

            assertTrue(fixture.loader.isListing)
            fixture.loader.load()

            eventually("the end of the listing") { !fixture.loader.isListing }
            fixture.awaitNames("audit-value")
        }

        @Test
        fun `should show an empty registry as an empty list`() {
            val fixture = fixtureFor(sourceOf())

            fixture.loader.load()

            eventually("the end of the listing") { !fixture.loader.isListing }
            assertEquals(emptyList<String>(), fixture.names())
            assertNull(fixture.selectedName())
        }

        @Test
        fun `should ask for the formats of the schema of the topic and the selected one first`() {
            topic = "orders"
            val source = sourceOf("audit-value", "orders-value", "payments-value")
            val fixture = fixtureFor(source)

            fixture.loader.load()

            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            eventually("the request for the formats") { source.resolveRequests.isNotEmpty() }
            val (names, first) = source.resolveRequests.first()
            // The schema of the topic is the selected one, and these two are not all the schemas.
            assertEquals(listOf("orders-value"), names)
            assertEquals(listOf("orders-value"), first)
        }

        @Test
        fun `should ask for the formats of no more than the selected schema and the one of the topic`() {
            topic = "orders"
            val names = (1..500).map { "subject-%03d-value".format(it) } + "orders-value"
            val source = FakeSource(names, allFormats)
            source.resolveGate.complete(Unit)
            val fixture = fixtureFor(source)

            fixture.loader.load()
            fixture.awaitNames(*names.sorted().toTypedArray())
            Thread.sleep(400)

            // The thousands of schemas that nobody looks at are not worth a request each.
            assertTrue(source.urgentNames().size <= 2, source.urgentNames().toString())
            assertTrue(source.resolveRequests.all { (requested, _) -> requested.size <= 2 }, source.resolveRequests.toString())
        }

        @Test
        fun `should not ask for formats of schemas while it fills the list`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)

            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")
            Thread.sleep(200)

            // Filling the list selects the first schema on the way, which is no reason to ask for its format.
            assertTrue(source.formatRequests.isEmpty(), source.formatRequests.toString())
        }
    }

    @Nested
    inner class `formats that are wanted` {

        private fun loaded(vararg names: String, setup: (FakeSource) -> Unit = {}): Pair<FakeSource, Fixture> {
            val source = sourceOf(*names)
            setup(source)
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames(*names.sorted().toTypedArray())
            return source to fixture
        }

        @Test
        fun `should fill in the format of a schema that is asked for`() {
            val (source, fixture) = loaded("audit-value", "new-value", "orders-value", "payments-value")

            fixture.loader.wantFormat(fixture.item("orders-value"))
            source.resolveGate.complete(Unit)

            eventually("the format") { fixture.formatNow("orders-value") != null }
            assertEquals(KafkaRegistryFormat.PROTOBUF, fixture.format("orders-value"))
        }

        @Test
        fun `should not fill in the formats of the schemas that nobody asked for`() {
            val (source, fixture) = loaded("audit-value", "new-value", "orders-value", "payments-value")

            fixture.loader.wantFormat(fixture.item("orders-value"))
            source.resolveGate.complete(Unit)
            eventually("the format") { fixture.formatNow("orders-value") != null }
            Thread.sleep(300)

            // The first schema is selected, so its format was asked for. The others were not.
            assertEquals(KafkaRegistryFormat.AVRO, fixture.format("audit-value"))
            assertNull(fixture.format("new-value"))
            assertNull(fixture.format("payments-value"))
        }

        @Test
        fun `should ask for the schemas that are asked for in a burst together`() {
            val (source, fixture) = loaded("audit-value", "new-value", "orders-value", "payments-value")
            val items = listOf("payments-value", "new-value", "orders-value").map { fixture.item(it) }

            items.forEach { fixture.loader.wantFormat(it) }

            eventually("the request") { source.wantedBatches().isNotEmpty() }
            assertEquals(listOf(listOf("payments-value", "new-value", "orders-value")), source.wantedBatches())
        }

        @Test
        fun `should ask once for a schema that is asked for again and again`() {
            val (source, fixture) = loaded("audit-value", "new-value")
            val item = fixture.item("new-value")

            repeat(30) { fixture.loader.wantFormat(item) }

            eventually("the request") { source.wantedBatches().isNotEmpty() }
            Thread.sleep(300)
            assertEquals(listOf(listOf("new-value")), source.wantedBatches())
        }

        @Test
        fun `should not ask for a schema that has its format`() {
            val (source, fixture) = loaded("audit-value", "new-value")
            val item = fixture.item("new-value")
            item.schemaFormat = KafkaRegistryFormat.JSON

            fixture.loader.wantFormat(item)
            Thread.sleep(400)

            assertTrue(source.wantedBatches().isEmpty(), source.wantedBatches().toString())
        }

        @Test
        fun `should only ask for the schemas that were asked for last`() {
            val names = (1..100).map { "subject-%03d-value".format(it) }
            val (source, fixture) = loaded(*names.toTypedArray())
            val items = fixture.items()

            // A list that is scrolled fast paints a row after another. The ones it has left behind are not worth it.
            items.forEach { fixture.loader.wantFormat(it) }

            eventually("the request") { source.wantedBatches().isNotEmpty() }
            assertEquals(names.takeLast(SchemaComboBoxLoader.MAX_WANTED), source.wantedBatches().first())
        }

        @Test
        fun `should ask again for schemas that are asked for after the first request`() {
            val (source, fixture) = loaded("audit-value", "new-value", "orders-value")
            source.resolveGate.complete(Unit)
            fixture.loader.wantFormat(fixture.item("new-value"))
            eventually("the first request") { fixture.formatNow("new-value") != null }

            fixture.loader.wantFormat(fixture.item("orders-value"))

            eventually("the second request") { fixture.formatNow("orders-value") != null }
            assertEquals(listOf(listOf("new-value"), listOf("orders-value")), source.wantedBatches())
        }

        @Test
        fun `should redraw once the formats are there`() {
            val (source, fixture) = loaded("audit-value", "new-value")
            source.resolveGate.complete(Unit)
            eventually("the format of the selected schema") { fixture.formatNow("audit-value") != null }
            Thread.sleep(200)
            val before = fixture.combo.repaints.get()

            fixture.loader.wantFormat(fixture.item("new-value"))

            eventually("the redraw") { fixture.formatNow("new-value") != null && fixture.combo.repaints.get() > before }
        }

        @Test
        fun `should go on after a request failed`() {
            val inner = sourceOf("audit-value", "new-value", "orders-value")
            inner.resolveGate.complete(Unit)
            val source = FailingOnceSource(inner)
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "new-value", "orders-value")

            fixture.loader.wantFormat(fixture.item("new-value"))
            eventually("the failed request") { source.failures.get() == 1 }
            fixture.loader.wantFormat(fixture.item("orders-value"))

            eventually("the format after the failure") { fixture.formatNow("orders-value") != null }
        }
    }

    // The first request for formats that are wanted fails, like a registry that is not reachable for a moment.
    private class FailingOnceSource(private val inner: FakeSource) : SchemaFormats by inner {
        val failures = AtomicInteger()

        override suspend fun resolveSchemaFormats(
            names: Collection<String>,
            first: Collection<String>,
            onResolved: (name: String, format: KafkaRegistryFormat) -> Unit
        ) {
            if (first.isEmpty() && failures.getAndIncrement() == 0) error("registry unavailable")
            inner.resolveSchemaFormats(names, first, onResolved)
        }
    }

    @Nested
    inner class `listing again` {

        @Test
        fun `should show the names that were listed before right away`() {
            val source = sourceOf("audit-value", "new-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            source.listingGate = CompletableDeferred()
            val fixture = fixtureFor(source)

            fixture.loader.load()

            // The registry has not answered yet.
            fixture.awaitNames("audit-value", "orders-value")
            assertFalse(fixture.loader.isListing)
        }

        @Test
        fun `should correct the list when the registry answers with other names`() {
            val source = sourceOf("audit-value", "new-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value", "removed-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "removed-value")

            gate.complete(Unit)

            fixture.awaitNames("audit-value", "new-value", "orders-value")
        }

        @Test
        fun `should keep the items of the list when the registry answers with the same names`() {
            val source = sourceOf("audit-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")
            val item = fixture.item("orders-value")

            gate.complete(Unit)
            eventually("the end of the listing") { source.nameRequests.get() == 1 }
            Thread.sleep(200)

            assertSame(item, fixture.item("orders-value"))
        }

        @Test
        fun `should keep the selection while the registry is asked`() {
            val source = sourceOf("audit-value", "new-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")
            fixture.select("orders-value")

            gate.complete(Unit)

            fixture.awaitNames("audit-value", "new-value", "orders-value")
            assertEquals("orders-value", fixture.selectedName())
        }

        @Test
        fun `should select the schema of the topic that only the current names have`() {
            topic = "orders"
            val source = sourceOf("audit-value", "orders-value", "payments-value")
            source.cachedNames = listOf("audit-value", "payments-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "payments-value")
            // The first schema is selected, because nothing suits the topic yet.
            assertEquals("audit-value", fixture.selectedName())

            gate.complete(Unit)

            eventually("the schema of the topic") { fixture.combo.item?.schemaName == "orders-value" }
        }

        @Test
        fun `should keep what is shown when the registry cannot be asked`() {
            val source = sourceOf("audit-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            source.names = null
            val fixture = fixtureFor(source)

            fixture.loader.load()
            eventually("the request") { source.nameRequests.get() == 1 }
            Thread.sleep(200)

            assertEquals(listOf("audit-value", "orders-value"), fixture.names())
            assertFalse(fixture.loader.isListing)
        }

        @Test
        fun `should keep what is shown when the registry cannot be asked for a reload`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")

            source.names = null
            fixture.loader.reload()
            eventually("the request") { source.nameRequests.get() == 2 }
            Thread.sleep(200)

            assertEquals(listOf("audit-value", "orders-value"), fixture.names())
        }

        @Test
        fun `should show an empty list when the registry cannot be asked and nothing was listed before`() {
            val source = sourceOf("audit-value")
            source.names = null
            val fixture = fixtureFor(source)

            fixture.loader.load()

            eventually("the end of the listing") { !fixture.loader.isListing }
            assertEquals(emptyList<String>(), fixture.names())
        }

        @Test
        fun `should keep a schema that was chosen although the names that were listed before lack it`() {
            val source = sourceOf("audit-value", "new-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            // What an editor does when it restores its settings: the schema is there before the list is.
            onEdt { KafkaEditorUtils.selectSchemaByName(fixture.combo, "new-value", KafkaRegistryFormat.AVRO) }

            fixture.loader.load()

            fixture.awaitNames("audit-value", "new-value", "orders-value")
            assertEquals("new-value", fixture.selectedName())
            gate.complete(Unit)
            eventually("the request") { source.nameRequests.get() == 1 }
            Thread.sleep(200)
            assertEquals("new-value", fixture.selectedName())
        }

        @Test
        fun `should drop a schema that was chosen when the registry says it is not there`() {
            val source = sourceOf("audit-value", "orders-value")
            source.cachedNames = listOf("audit-value", "orders-value")
            val gate = CompletableDeferred<Unit>().also { source.listingGate = it }
            val fixture = fixtureFor(source)
            onEdt { KafkaEditorUtils.selectSchemaByName(fixture.combo, "gone-value", KafkaRegistryFormat.AVRO) }
            fixture.loader.load()
            fixture.awaitNames("audit-value", "gone-value", "orders-value")

            gate.complete(Unit)

            fixture.awaitNames("audit-value", "orders-value")
            // Another schema must not take its place unnoticed.
            assertNull(fixture.selectedName())
        }
    }

    @Nested
    inner class `selection` {

        @Test
        fun `should select the value schema of the topic`() {
            topic = "orders"
            val fixture = fixtureFor(sourceOf("audit-value", "orders-key", "orders-value"))

            fixture.loader.load()

            eventually("the selection") { fixture.combo.item?.schemaName == "orders-value" }
        }

        @Test
        fun `should select the key schema of the topic for a key`() {
            topic = "orders"
            val fixture = fixtureFor(sourceOf("audit-value", "orders-key", "orders-value"), isKey = true)

            fixture.loader.load()

            eventually("the selection") { fixture.combo.item?.schemaName == "orders-key" }
        }

        @Test
        fun `should keep what was picked when the list is reloaded`() {
            topic = "orders"
            val source = sourceOf("audit-value", "orders-value", "payments-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            fixture.select("payments-value")

            source.names = listOf("audit-value", "new-value", "orders-value", "payments-value")
            fixture.loader.reload()

            fixture.awaitNames("audit-value", "new-value", "orders-value", "payments-value")
            assertEquals("payments-value", fixture.selectedName())
        }

        @Test
        fun `should suggest the schema of the topic when it turns up although another one was selected for lack of it`() {
            topic = "orders"
            val source = sourceOf("audit-value", "payments-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "payments-value")
            // The first schema is selected, but nobody chose it.
            assertEquals("audit-value", fixture.selectedName())

            source.names = listOf("audit-value", "orders-value", "payments-value")
            fixture.loader.reload()

            eventually("the schema of the topic") { fixture.combo.item?.schemaName == "orders-value" }
        }

        @Test
        fun `should not select another schema when the one that was picked is gone`() {
            val source = sourceOf("audit-value", "orders-value", "payments-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            fixture.select("orders-value")

            source.names = listOf("audit-value", "payments-value")
            fixture.loader.reload()

            fixture.awaitNames("audit-value", "payments-value")
            assertNull(fixture.selectedName())
        }

        @Test
        fun `should select a schema again that comes back after it was gone`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")
            fixture.select("orders-value")
            source.names = listOf("audit-value")
            fixture.loader.reload()
            fixture.awaitNames("audit-value")
            assertNull(fixture.selectedName())

            source.names = listOf("audit-value", "orders-value")
            fixture.loader.reload()

            eventually("the schema that was picked") { fixture.combo.item?.schemaName == "orders-value" }
        }

        @Test
        fun `should prefer the schema of another topic to what was picked before`() {
            topic = "orders"
            val fixture = fixtureFor(sourceOf("audit-value", "orders-value", "payments-value"))
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            fixture.select("audit-value")

            topic = "payments"
            fixture.loader.load(topicChanged = true)

            eventually("the schema of the new topic") { fixture.combo.item?.schemaName == "payments-value" }
        }

        @Test
        fun `should keep what was picked when the other topic has no schema`() {
            topic = "orders"
            val fixture = fixtureFor(sourceOf("audit-value", "orders-value", "payments-value"))
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            fixture.select("audit-value")

            topic = "unrelated"
            fixture.loader.load(topicChanged = true)
            Thread.sleep(200)

            assertEquals("audit-value", fixture.selectedName())
        }

        @Test
        fun `should not lose the topic change when a plain reload comes in`() {
            topic = "orders"
            val fixture = fixtureFor(sourceOf("audit-value", "orders-value", "payments-value"))
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")
            fixture.select("audit-value")

            topic = "payments"
            fixture.loader.load(topicChanged = true)
            fixture.loader.reload()

            eventually("the schema of the new topic") { fixture.combo.item?.schemaName == "payments-value" }
        }

        @Test
        fun `should keep the schema that is chosen after the topic changed`() {
            topic = "orders"
            val source = sourceOf("audit-value", "orders-value", "payments-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value", "payments-value")

            // What an editor does when it restores its settings: the topic first, then its schema.
            topic = "payments"
            onEdt {
                fixture.loader.load(topicChanged = true)
                KafkaEditorUtils.selectSchemaByName(fixture.combo, "audit-value", KafkaRegistryFormat.AVRO)
            }

            source.awaitListedAgain()
            assertEquals("audit-value", fixture.selectedName())
        }

        @Test
        fun `should keep the saved schema of an editor that is restored right after it was created`() {
            topic = "orders"
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()

            onEdt {
                fixture.loader.load(topicChanged = true)
                KafkaEditorUtils.selectSchemaByName(fixture.combo, "audit-value", KafkaRegistryFormat.AVRO)
            }

            fixture.awaitNames("audit-value", "orders-value")
            eventually("the list to be shown with the saved schema") {
                source.resolveRequests.any { (_, first) -> first.firstOrNull() == "audit-value" }
            }
            assertEquals("audit-value", fixture.selectedName())
        }

        @Test
        fun `should find the schema again that was selected before the list arrived`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            // What the consumer does when it restores its settings.
            onEdt { KafkaEditorUtils.selectSchemaByName(fixture.combo, "orders-value", KafkaRegistryFormat.PROTOBUF) }

            fixture.loader.load()

            fixture.awaitNames("audit-value", "orders-value")
            assertEquals("orders-value", fixture.selectedName())
        }

        @Test
        fun `should leave the first schema selected when nothing says which one is meant`() {
            val fixture = fixtureFor(sourceOf("payments-value", "audit-value"))

            fixture.loader.load()

            eventually("the selection") { fixture.combo.item?.schemaName == "audit-value" }
        }
    }

    @Nested
    inner class `formats` {

        @Test
        fun `should keep the formats it found when the list is shown again`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")
            val item = onEdt { fixture.combo.model.getElementAt(0) }
            source.resolveGate.complete(Unit)
            eventually("the format") { item.schemaFormat != null }

            fixture.loader.reload()
            fixture.loader.load()
            Thread.sleep(200)

            assertSame(item, onEdt { fixture.combo.model.getElementAt(0) })
            assertEquals(KafkaRegistryFormat.AVRO, item.schemaFormat)
        }

        @Test
        fun `should resolve the selected schema right away`() {
            val source = sourceOf("audit-value", "orders-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value", "orders-value")

            fixture.select("orders-value")

            eventually("the format of the selected schema") { fixture.formatNow("orders-value") != null }
            assertEquals(KafkaRegistryFormat.PROTOBUF, fixture.format("orders-value"))
            assertTrue("orders-value" in source.formatRequests)
        }

        @Test
        fun `should replace a format it only guessed with the one the registry knows`() {
            val source = sourceOf("audit-value")
            val fixture = fixtureFor(source)
            onEdt { KafkaEditorUtils.selectSchemaByName(fixture.combo, "audit-value", KafkaRegistryFormat.JSON) }
            source.resolveGate.complete(Unit)

            fixture.loader.load()

            eventually("the real format") { fixture.formatNow("audit-value") == KafkaRegistryFormat.AVRO }
        }

        @Test
        fun `should take the formats that are known without asking for them`() {
            val source = sourceOf("audit-value", "orders-value")
            source.resolveGate.complete(Unit)
            runCatching { kotlinx.coroutines.runBlocking { source.getSchemaFormat("orders-value") } }
            val fixture = fixtureFor(source)

            fixture.loader.load()

            fixture.awaitNames("audit-value", "orders-value")
            assertEquals(KafkaRegistryFormat.PROTOBUF, fixture.format("orders-value"))
        }
    }

    @Nested
    inner class `reloading` {

        @Test
        fun `should list a burst of reloads once`() {
            val source = sourceOf("audit-value")
            val fixture = fixtureFor(source, reloadDelayMs = 100)
            fixture.loader.load()
            fixture.awaitNames("audit-value")
            val before = source.nameRequests.get()

            repeat(10) { fixture.loader.reload() }
            Thread.sleep(500)

            assertEquals(before + 1, source.nameRequests.get())
        }

        @Test
        fun `should show names that were added to the registry`() {
            val source = sourceOf("audit-value")
            val fixture = fixtureFor(source)
            fixture.loader.load()
            fixture.awaitNames("audit-value")

            source.names = listOf("audit-value", "new-value")
            fixture.loader.reload()

            fixture.awaitNames("audit-value", "new-value")
        }
    }
}
