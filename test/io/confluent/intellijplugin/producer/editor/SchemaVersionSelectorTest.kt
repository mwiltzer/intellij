package io.confluent.intellijplugin.producer.editor

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

class SchemaVersionSelectorTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    /** What the registry has for each schema, which the tests change, and what was asked of it. */
    private class Registry {
        val versions = ConcurrentHashMap<String, List<Long>>()
        val asked = ConcurrentLinkedQueue<String>()
        val gates = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

        // Schemas for which asking the registry blocks the thread until the latch opens, which cannot be stopped.
        val blocking = ConcurrentHashMap<String, CountDownLatch>()

        // The requests that were told to stop.
        val cancelled = ConcurrentLinkedQueue<String>()

        // Schemas for which the registry cannot be asked: it answers with `null`, or fails.
        val unavailable = ConcurrentHashMap.newKeySet<String>()
        val failing = ConcurrentHashMap.newKeySet<String>()

        suspend fun load(name: String): List<Long>? {
            asked += name
            // What the registry has when it is asked, however late it comes back with that.
            val answer = versions[name] ?: emptyList()
            try {
                blocking[name]?.await()
                gates[name]?.await()
            } catch (e: CancellationException) {
                cancelled += name
                throw e
            }
            if (name in failing) error("registry unavailable")
            if (name in unavailable) return null
            return answer
        }
    }

    private inner class Fixture(settleMillis: Long = 20) {
        val registry = Registry()
        val changes = CopyOnWriteArrayList<SchemaVersionState>()
        val selector = SchemaVersionSelector(scope, registry::load, settleMillis) { changes += it }

        /** Shows the schema and waits until the selector took it as the schema of its choice. */
        fun show(name: String?) {
            selector.schemaSelected(name)
            eventually { selector.state().schemaName == name?.takeIf { it.isNotEmpty() } }
        }

        /** Shows the schema, which has the [versions], and waits for them. */
        fun showWithVersions(name: String, vararg versions: Long) {
            registry.versions[name] = versions.toList()
            show(name)
            selector.reload()
            eventually { selector.state().versions == versions.toList().sortedDescending() }
        }

        fun awaitAsked(count: Int) = eventually { registry.asked.size >= count }
    }

    private fun eventually(condition: () -> Boolean) = runBlocking {
        withTimeout(5_000) { while (!condition()) delay(5) }
    }

    @Nested
    inner class State {

        @Test
        fun `should offer the latest version, then the versions, the newest first`() {
            assertEquals(listOf(null, 5L, 3L, 1L), SchemaVersionState("a", listOf(1, 5, 3), null).choices)
        }

        @Test
        fun `should keep a chosen version in the list until the registry says it is gone`() {
            assertEquals(listOf(null, 7L, 2L, 1L), SchemaVersionState("a", listOf(2, 1), 7).choices)
            assertEquals(listOf(null, 2L, 1L), SchemaVersionState("a", listOf(2, 1), 2).choices)
        }

        @Test
        fun `should offer the latest version alone while the versions are not known`() {
            assertEquals(listOf<Long?>(null), SchemaVersionState("a", emptyList(), null).choices)
            assertNull(SchemaVersionState("a", emptyList(), null).latest)
        }

        @Test
        fun `should name the newest version the latest one`() {
            assertEquals(5L, SchemaVersionState("a", listOf(5, 3, 1), null).latest)
        }
    }

    @Nested
    inner class ChoosingForASchema {

        @Test
        fun `should write with the latest version at first`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should write with the version that was chosen`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            fixture.selector.choose(3)

            assertEquals(3L, fixture.selector.selectedFor("orders-value"))
            assertEquals(3L, fixture.selector.state().selected)
        }

        @Test
        fun `should go back to the latest version`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.choose(3)

            fixture.selector.choose(null)

            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should not write another schema with the chosen version`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.choose(3)

            assertNull(fixture.selector.selectedFor("other-value"))
            assertNull(fixture.selector.selectedFor(null))
            assertNull(fixture.selector.selectedFor(""))
        }

        @Test
        fun `should start another schema with the latest version`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.choose(3)

            fixture.show("other-value")

            assertNull(fixture.selector.selectedFor("other-value"))
            assertNull(fixture.selector.state().selected)
        }

        @Test
        fun `should write a schema that is about to be shown with the latest version`() {
            val fixture = Fixture(settleMillis = 400)
            fixture.show("orders-value")
            fixture.selector.choose(3)

            // The change to the other schema is under way, but did not settle yet.
            fixture.selector.schemaSelected("other-value")

            assertNull(fixture.selector.selectedFor("other-value"))
            assertEquals(3L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should tell about a choice, but not about a choice that is made again`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.changes.clear()

            fixture.selector.choose(3)
            fixture.selector.choose(3)

            assertEquals(listOf(3L), fixture.changes.map { it.selected })
        }

        @Test
        fun `should ignore a choice when there is no schema`() {
            val fixture = Fixture()

            fixture.selector.choose(3)

            assertNull(fixture.selector.state().selected)
            assertTrue(fixture.changes.isEmpty())
        }
    }

    @Nested
    inner class SchemaListBeingRebuilt {

        @Test
        fun `should keep the choice when the list selects other schemas on the way and then the same one`() {
            val fixture = Fixture(settleMillis = 80)
            fixture.show("orders-value")
            fixture.selector.choose(3)
            fixture.changes.clear()

            // What rebuilding the combo box does: nothing is selected, the first item, then the item that was.
            fixture.selector.schemaSelected(null)
            fixture.selector.schemaSelected("a-first-schema")
            fixture.selector.schemaSelected("orders-value")
            Thread.sleep(300)

            assertEquals(3L, fixture.selector.selectedFor("orders-value"))
            assertEquals("orders-value", fixture.selector.state().schemaName)
            assertTrue(fixture.changes.isEmpty())
        }

        @Test
        fun `should wait until the changes stop, however long they go on`() {
            val fixture = Fixture(settleMillis = 400)
            fixture.show("orders-value")
            fixture.selector.choose(3)
            fixture.changes.clear()

            // Each change comes before the one before it settled, but together they take longer than that.
            fixture.selector.schemaSelected(null)
            Thread.sleep(250)
            fixture.selector.schemaSelected("a-first-schema")
            Thread.sleep(250)
            fixture.selector.schemaSelected("orders-value")
            Thread.sleep(700)

            assertEquals(3L, fixture.selector.selectedFor("orders-value"))
            assertEquals("orders-value", fixture.selector.state().schemaName)
            assertTrue(fixture.changes.isEmpty())
        }

        @Test
        fun `should ask the registry for the schema that is shown, not for the ones that only pass by`() {
            val fixture = Fixture(settleMillis = 80)
            fixture.show("orders-value")

            fixture.selector.schemaSelected("a-first-schema")
            fixture.selector.schemaSelected("orders-value")
            fixture.selector.reload()
            Thread.sleep(250)

            assertEquals(listOf("orders-value"), fixture.registry.asked.toList())
        }

        @Test
        fun `should take another schema once it stays`() {
            val fixture = Fixture(settleMillis = 40)
            fixture.show("orders-value")
            fixture.selector.choose(3)

            fixture.selector.schemaSelected("a-first-schema")
            fixture.selector.schemaSelected("other-value")
            eventually { fixture.selector.state().schemaName == "other-value" }

            assertNull(fixture.selector.state().selected)
        }

        @Test
        fun `should take no schema when none is selected`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            fixture.show(null)

            assertNull(fixture.selector.state().schemaName)
            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should take an empty name as no schema`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            fixture.show("")

            assertNull(fixture.selector.state().schemaName)
            assertNull(fixture.selector.selectedFor(""))
        }
    }

    @Nested
    inner class Restoring {

        @Test
        fun `should bring back a choice for the schema that is shown`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            fixture.selector.restore("orders-value", 2)

            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
            assertEquals(listOf(null, 2L), fixture.selector.state().choices)
        }

        @Test
        fun `should not tell about a choice that is brought back again`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.restore("orders-value", 2)
            fixture.changes.clear()

            fixture.selector.restore("orders-value", 2)

            assertTrue(fixture.changes.isEmpty())
            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should bring back a choice for the schema that is about to be shown`() {
            val fixture = Fixture(settleMillis = 40)
            fixture.show("other-value")

            fixture.selector.schemaSelected("orders-value")
            fixture.selector.restore("orders-value", 2)
            // Before it settles, a record of the schema that is shown is written with the choice that was saved.
            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
            eventually { fixture.selector.state().schemaName == "orders-value" }

            assertEquals(2L, fixture.selector.state().selected)
            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should bring back a choice when the restore comes before the schema is shown`() {
            val fixture = Fixture(settleMillis = 40)

            fixture.selector.restore("orders-value", 2)
            fixture.selector.schemaSelected("orders-value")
            eventually { fixture.selector.state().schemaName == "orders-value" }

            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should drop a saved choice when another schema is shown`() {
            val fixture = Fixture()
            fixture.show("other-value")
            fixture.selector.restore("orders-value", 2)

            fixture.show("third-value")

            assertNull(fixture.selector.selectedFor("orders-value"))
            assertNull(fixture.selector.selectedFor("third-value"))
            fixture.show("orders-value")
            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should bring back the latest version, which replaces the choice that was made`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.choose(3)

            fixture.selector.restore("orders-value", null)

            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should ignore a saved choice without a schema`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.changes.clear()

            fixture.selector.restore("", 2)

            assertTrue(fixture.changes.isEmpty())
            assertNull(fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should show a saved choice before the registry was asked`() {
            val fixture = Fixture()
            fixture.show("orders-value")

            fixture.selector.restore("orders-value", 2)

            assertTrue(fixture.registry.asked.isEmpty())
            assertEquals(2L, fixture.selector.state().selected)
        }
    }

    @Nested
    inner class Reloading {

        @Test
        fun `should list the versions of the schema, the newest first`() {
            val fixture = Fixture()

            fixture.showWithVersions("orders-value", 1, 3, 2)

            assertEquals(listOf(3L, 2L, 1L), fixture.selector.state().versions)
            assertEquals(listOf(null, 3L, 2L, 1L), fixture.selector.state().choices)
            assertEquals(3L, fixture.selector.state().latest)
        }

        @Test
        fun `should not ask the registry before it is told to`() {
            val fixture = Fixture()

            fixture.show("orders-value")
            Thread.sleep(100)

            assertTrue(fixture.registry.asked.isEmpty())
        }

        @Test
        fun `should not ask the registry when there is no schema`() {
            val fixture = Fixture()

            fixture.selector.reload()
            Thread.sleep(100)

            assertTrue(fixture.registry.asked.isEmpty())
        }

        @Test
        fun `should list the versions again when asked again`() {
            val fixture = Fixture()
            fixture.showWithVersions("orders-value", 1, 2)

            fixture.registry.versions["orders-value"] = listOf(1, 2, 3)
            fixture.selector.reload()

            eventually { fixture.selector.state().versions == listOf(3L, 2L, 1L) }
        }

        @Test
        fun `should keep the choice when the registry still has the version`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.choose(2)
            fixture.registry.versions["orders-value"] = listOf(1, 2, 3)

            fixture.selector.reload()
            eventually { fixture.selector.state().versions.isNotEmpty() }

            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should go back to the latest version when the registry has not got the chosen one`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.restore("orders-value", 9)
            fixture.registry.versions["orders-value"] = listOf(1, 2, 3)

            fixture.selector.reload()
            eventually { fixture.selector.state().versions.isNotEmpty() }

            assertNull(fixture.selector.selectedFor("orders-value"))
            assertEquals(listOf(null, 3L, 2L, 1L), fixture.selector.state().choices)
        }

        @Test
        fun `should keep what it knows when the registry could not be asked`() {
            val fixture = Fixture()
            fixture.showWithVersions("orders-value", 1, 2)
            fixture.selector.choose(1)
            fixture.registry.unavailable += "orders-value"

            fixture.selector.reload()
            fixture.awaitAsked(2)
            Thread.sleep(50)

            assertEquals(listOf(2L, 1L), fixture.selector.state().versions)
            assertEquals(1L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should keep what it knows when asking the registry fails`() {
            val fixture = Fixture()
            fixture.showWithVersions("orders-value", 1, 2)
            fixture.selector.choose(1)
            fixture.registry.failing += "orders-value"

            fixture.selector.reload()
            fixture.awaitAsked(2)
            Thread.sleep(50)

            assertEquals(listOf(2L, 1L), fixture.selector.state().versions)
            assertEquals(1L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should keep a saved choice when the registry could not be asked`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.selector.restore("orders-value", 2)
            fixture.registry.failing += "orders-value"

            fixture.selector.reload()
            fixture.awaitAsked(1)
            Thread.sleep(50)

            assertEquals(2L, fixture.selector.selectedFor("orders-value"))
        }

        @Test
        fun `should not show the versions of a schema that is not shown any longer`() {
            val fixture = Fixture()
            fixture.registry.versions["orders-value"] = listOf(1, 2)
            val gate = CompletableDeferred<Unit>().also { fixture.registry.gates["orders-value"] = it }
            fixture.show("orders-value")
            fixture.selector.reload()
            fixture.awaitAsked(1)

            fixture.show("other-value")
            gate.complete(Unit)
            Thread.sleep(150)

            assertEquals("other-value", fixture.selector.state().schemaName)
            assertTrue(fixture.selector.state().versions.isEmpty())
        }

        @Test
        fun `should not show the versions of a schema that is not shown any longer when asking cannot be stopped`() {
            val fixture = Fixture()
            fixture.registry.versions["orders-value"] = listOf(1, 2)
            // A blocking call of the registry is not interrupted by the schema going away, it just comes back.
            val latch = CountDownLatch(1).also { fixture.registry.blocking["orders-value"] = it }
            fixture.show("orders-value")
            fixture.selector.reload()
            fixture.awaitAsked(1)

            fixture.show("other-value")
            latch.countDown()
            Thread.sleep(150)

            assertEquals("other-value", fixture.selector.state().schemaName)
            assertTrue(fixture.selector.state().versions.isEmpty())
        }

        @Test
        fun `should stop asking for the versions of a schema that is not shown any longer`() {
            val fixture = Fixture()
            val gate = CompletableDeferred<Unit>().also { fixture.registry.gates["orders-value"] = it }
            fixture.show("orders-value")
            fixture.selector.reload()
            fixture.awaitAsked(1)

            fixture.show("other-value")

            eventually { fixture.registry.cancelled.toList() == listOf("orders-value") }
            gate.complete(Unit)
        }

        @Test
        fun `should show only the latest answer when asked again before the first came`() {
            val fixture = Fixture()
            fixture.registry.versions["orders-value"] = listOf(1)
            val gate = CompletableDeferred<Unit>().also { fixture.registry.gates["orders-value"] = it }
            fixture.show("orders-value")
            fixture.selector.reload()
            fixture.awaitAsked(1)

            fixture.registry.gates.clear()
            fixture.registry.versions["orders-value"] = listOf(1, 2)
            fixture.selector.reload()
            eventually { fixture.selector.state().versions == listOf(2L, 1L) }
            gate.complete(Unit)
            Thread.sleep(100)

            assertEquals(listOf(2L, 1L), fixture.selector.state().versions)
        }

        @Test
        fun `should tell about the versions it found`() {
            val fixture = Fixture()
            fixture.show("orders-value")
            fixture.changes.clear()
            fixture.registry.versions["orders-value"] = listOf(1, 2)

            fixture.selector.reload()
            eventually { fixture.changes.isNotEmpty() }

            assertEquals(listOf(2L, 1L), fixture.changes.last().versions)
        }

        @Test
        fun `should not tell about versions that did not change`() {
            val fixture = Fixture()
            fixture.showWithVersions("orders-value", 1, 2)
            fixture.changes.clear()

            fixture.selector.reload()
            fixture.awaitAsked(2)
            Thread.sleep(50)

            assertTrue(fixture.changes.isEmpty())
        }
    }
}
