package io.confluent.intellijplugin.data

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.core.monitoring.data.model.ObjectDataModel
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

@TestApplication
class SchemaRegistryListTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    /** Registry stand-in for the details: every schema is Avro, version 1, unless it is told to fail. */
    private class Registry(private val failing: Set<String> = emptySet()) {
        val requested = ConcurrentLinkedQueue<String>()
        private val gates = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

        fun hold(vararg names: String) = CompletableDeferred<Unit>().also { gate -> names.forEach { gates[it] = gate } }

        suspend fun load(name: String): KafkaSchemaInfo {
            requested += name
            gates[name]?.await()
            if (name in failing) error("registry unavailable")
            return KafkaSchemaInfo(name, KafkaRegistryFormat.AVRO, 1)
        }
    }

    private inner class Fixture(
        names: List<String> = (1..10).map { "subject-%02d".format(it) },
        registry: Registry = Registry(),
        detailsDelayMillis: Long = 0
    ) {
        val registry = registry
        val resolver = SchemaFormatResolver(4, scope) { registry.load(it) }

        // Changed by the tests while the list reads them.
        @Volatile
        var listing: SchemaListing? = SchemaListing(names.map { KafkaSchemaInfo(it) })

        @Volatile
        var settings = SchemaListSettings(filter = null, pinned = emptySet(), favoritesOnly = false, limit = null)
        val refreshes = AtomicInteger()

        val model = ObjectDataModel<KafkaSchemaInfo>(KafkaSchemaInfo::name) { emptyList<KafkaSchemaInfo>() to false }
        val list = SchemaRegistryList(
            scope = scope,
            resolver = resolver,
            cachedListing = { listing },
            settings = { settings },
            model = { model },
            refreshFromRegistry = { refreshes.incrementAndGet() },
            detailsDelayMillis = detailsDelayMillis
        )

        val shownNames get() = model.data?.map { it.name }
        val requestedNames get() = registry.requested.toList().sorted()

        fun shownWithDetails() = model.data.orEmpty().filter { it.type == KafkaRegistryFormat.AVRO }.map { it.name }
    }

    private fun eventually(condition: () -> Boolean) = runBlocking {
        withTimeout(5_000) { while (!condition()) delay(5) }
    }

    @Nested
    inner class View {

        @Test
        fun `should make the list out of the listing and the settings`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(filter = "subject-0", limit = 4)

            val (shown, hasMore) = fixture.list.view(fixture.listing!!)

            assertEquals(listOf("subject-01", "subject-02", "subject-03", "subject-04"), shown.map { it.name })
            assertTrue(hasMore)
        }

        @Test
        fun `should show the details that are known already`() = runBlocking<Unit> {
            val fixture = Fixture()
            fixture.resolver.refreshAll(listOf("subject-02"))

            val (shown, _) = fixture.list.view(fixture.listing!!)

            assertEquals(KafkaSchemaInfo("subject-02", KafkaRegistryFormat.AVRO, 1), shown[1])
            assertNull(shown[0].type)
        }

        @Test
        fun `should keep the details the model shows`() {
            val fixture = Fixture()
            val detailed = KafkaSchemaInfo("subject-03", KafkaRegistryFormat.PROTOBUF, 7, compatibility = "FULL")
            fixture.model.setData(listOf(detailed))

            val (shown, _) = fixture.list.view(fixture.listing!!)

            assertEquals(detailed, shown[2])
        }

        @Test
        fun `should keep the favorite mark of the settings when it takes details from elsewhere`() = runBlocking<Unit> {
            val fixture = Fixture()
            fixture.resolver.refreshAll(listOf("subject-05"))
            fixture.settings = fixture.settings.copy(pinned = setOf("subject-05"))

            val (shown, _) = fixture.list.view(fixture.listing!!)

            assertEquals(KafkaSchemaInfo("subject-05", KafkaRegistryFormat.AVRO, 1, isFavorite = true), shown.first())
        }
    }

    @Nested
    inner class Refilter {

        @Test
        fun `should ask the registry when nothing was listed yet`() {
            val fixture = Fixture()
            fixture.listing = null

            fixture.list.refilter()

            eventually { fixture.refreshes.get() == 1 }
            assertNull(fixture.model.data)
        }

        @Test
        fun `should show the listing with the new settings without asking the registry`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(filter = "-07")

            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("subject-07") }
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should tell the model that there are more than the limit shows`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(limit = 3)

            fixture.list.refilter()

            eventually { fixture.shownNames?.size == 3 }
            assertTrue(fixture.model.loadMore)

            fixture.settings = fixture.settings.copy(limit = null)
            fixture.list.refilter()

            eventually { fixture.shownNames?.size == 10 }
            assertFalse(fixture.model.loadMore)
        }

        @Test
        fun `should show the pinned schemas first`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(pinned = setOf("subject-09"))

            fixture.list.refilter()

            eventually { fixture.shownNames?.firstOrNull() == "subject-09" }
            assertTrue(fixture.model.data!!.first().isFavorite)
        }

        @Test
        fun `should apply the latest settings when changes come in at once`() {
            val fixture = Fixture()
            repeat(50) { index ->
                fixture.settings = fixture.settings.copy(filter = if (index % 2 == 0) "-01" else "-02")
                fixture.list.refilter()
            }
            fixture.settings = fixture.settings.copy(filter = "-05")
            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("subject-05") }
        }

        @Test
        fun `should load the details of the schemas that are shown`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(filter = "-0", limit = 3)

            fixture.list.refilter()

            eventually { fixture.shownWithDetails().size == 3 }
            assertEquals(listOf("subject-01", "subject-02", "subject-03"), fixture.requestedNames)
            assertEquals(listOf("subject-01", "subject-02", "subject-03"), fixture.shownNames)
        }

        @Test
        fun `should show the schemas at once and the details later`() {
            val registry = Registry()
            val gate = registry.hold("subject-01", "subject-02")
            val fixture = Fixture(registry = registry)
            fixture.settings = fixture.settings.copy(limit = 2)

            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("subject-01", "subject-02") }
            assertTrue(fixture.shownWithDetails().isEmpty())

            gate.complete(Unit)
            eventually { fixture.shownWithDetails().size == 2 }
        }

        @Test
        fun `should not ask again for the details of schemas it showed before`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(limit = 3)
            fixture.list.refilter()
            eventually { fixture.shownWithDetails().size == 3 }

            fixture.settings = fixture.settings.copy(limit = 5)
            fixture.list.refilter()

            eventually { fixture.shownWithDetails().size == 5 }
            assertEquals((1..5).map { "subject-%02d".format(it) }, fixture.requestedNames)
        }

        @Test
        fun `should show the known details at once when the list comes back`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(filter = "-01")
            fixture.list.refilter()
            eventually { fixture.shownWithDetails() == listOf("subject-01") }
            fixture.settings = fixture.settings.copy(filter = "-02")
            fixture.list.refilter()
            eventually { fixture.shownWithDetails() == listOf("subject-02") }
            val requests = fixture.registry.requested.size

            fixture.settings = fixture.settings.copy(filter = "-01")
            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("subject-01") }
            // No wait: the details are there when the list is shown.
            assertEquals(listOf("subject-01"), fixture.shownWithDetails())
            assertEquals(requests, fixture.registry.requested.size)
        }

        @Test
        fun `should not load details for a list that was replaced while the filter was typed`() {
            val fixture = Fixture(detailsDelayMillis = 1_000)
            fixture.settings = fixture.settings.copy(filter = "-0")
            fixture.list.refilter()
            eventually { fixture.shownNames?.size == 9 }

            fixture.settings = fixture.settings.copy(filter = "-10")
            fixture.list.refilter()

            eventually { fixture.shownWithDetails() == listOf("subject-10") }
            assertEquals(listOf("subject-10"), fixture.requestedNames)
        }

        @Test
        fun `should not bring back schemas of an earlier filter with details that come late`() = runBlocking<Unit> {
            val registry = Registry()
            val gate = registry.hold("subject-01", "subject-02")
            val fixture = Fixture(registry = registry)
            fixture.settings = fixture.settings.copy(filter = "-0", limit = 2)
            fixture.list.refilter()
            eventually { registry.requested.size == 2 }

            fixture.settings = fixture.settings.copy(filter = "-10", limit = 2)
            fixture.list.refilter()
            eventually { fixture.shownNames == listOf("subject-10") }
            gate.complete(Unit)
            eventually { fixture.shownWithDetails() == listOf("subject-10") }
            delay(100)

            assertEquals(listOf("subject-10"), fixture.shownNames)
        }

        @Test
        fun `should go on when the details of a schema can not be loaded`() {
            val fixture = Fixture(registry = Registry(failing = setOf("subject-02")))
            fixture.settings = fixture.settings.copy(limit = 3)

            fixture.list.refilter()

            eventually { fixture.shownWithDetails().size == 2 }
            assertEquals(listOf("subject-01", "subject-02", "subject-03"), fixture.shownNames)
            assertEquals(listOf("subject-01", "subject-03"), fixture.shownWithDetails())
        }

        @Test
        fun `should show an empty list when the registry has no schemas`() {
            val fixture = Fixture(names = emptyList())

            fixture.list.refilter()

            eventually { fixture.model.data == emptyList<KafkaSchemaInfo>() }
            assertTrue(fixture.registry.requested.isEmpty())
        }
    }

    @Nested
    inner class MergeLoaded {

        @Test
        fun `should merge the details into the list the model has now`() {
            val fixture = Fixture()
            fixture.model.setData(listOf(KafkaSchemaInfo("subject-02"), KafkaSchemaInfo("subject-03")))
            val loaded = listOf("subject-01", "subject-02").map { KafkaSchemaInfo(it, KafkaRegistryFormat.AVRO, 4) }

            val merged = fixture.list.mergeLoaded(fixture.model, loaded)

            // subject-01 belongs to a list the model had before.
            assertEquals(listOf("subject-02", "subject-03"), merged.map { it.name })
            assertEquals(4L, merged[0].version)
            assertNull(merged[1].version)
            assertEquals(merged, fixture.model.data)
        }

        @Test
        fun `should keep the favorite marks of the list the model has now`() {
            val fixture = Fixture()
            fixture.model.setData(listOf(KafkaSchemaInfo("subject-02", isFavorite = true)))

            val merged = fixture.list.mergeLoaded(fixture.model, listOf(KafkaSchemaInfo("subject-02", KafkaRegistryFormat.AVRO, 4)))

            assertTrue(merged.single().isFavorite)
            assertEquals(4L, merged.single().version)
        }

        @Test
        fun `should hand the loaded details back when the model has no list`() {
            val fixture = Fixture()
            val loaded = listOf(KafkaSchemaInfo("subject-01", KafkaRegistryFormat.AVRO, 4))

            val merged = fixture.list.mergeLoaded(fixture.model, loaded)

            assertSame(loaded, merged)
            assertNull(fixture.model.data)
        }

        @Test
        fun `should leave a list as it is when no details were loaded`() {
            val current = listOf(KafkaSchemaInfo("subject-02"))

            assertSame(current, SchemaRegistryList.mergeDetails(current, emptyList()))
        }
    }
}
