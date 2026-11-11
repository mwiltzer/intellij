package io.confluent.intellijplugin.data

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.core.monitoring.data.model.ObjectDataModel
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
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
import java.util.concurrent.CopyOnWriteArrayList
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
        detailsDelayMillis: Long = 0,
        prefixSettleMillis: Long = 0,
        prepare: suspend () -> Unit = {}
    ) {
        val registry = registry
        val resolver = SchemaFormatResolver(4, scope) { registry.load(it) }

        // Changed by the tests while the list reads them.
        @Volatile
        var listing: SchemaListing? = SchemaListing(names.map { KafkaSchemaInfo(it) })

        @Volatile
        var settings = SchemaListSettings(filter = null, pinned = emptySet(), favoritesOnly = false, limit = null)
        val refreshes = AtomicInteger()

        // What the registry listed for prefixes, as far as the list knows it, and what it was asked for by prefix.
        val prefixListings = CopyOnWriteArrayList<SchemaListing>()
        val fetches = ConcurrentLinkedQueue<String>()

        @Volatile
        var cached: (String?) -> SchemaListing? = { prefix ->
            SchemaListings.bestFor(prefix, listOfNotNull(listing) + prefixListings)
        }

        // A registry that can not list by prefix, unless a test says otherwise.
        @Volatile
        var fetch: suspend (String) -> SchemaListing? = { null }

        val model = ObjectDataModel<KafkaSchemaInfo>(KafkaSchemaInfo::name) { emptyList<KafkaSchemaInfo>() to false }
        val list = SchemaRegistryList(
            scope = scope,
            resolver = resolver,
            cachedListing = { prefix -> cached(prefix) },
            settings = { settings },
            model = { model },
            refreshFromRegistry = { refreshes.incrementAndGet() },
            detailsDelayMillis = detailsDelayMillis,
            prepare = prepare,
            fetchListing = { prefix ->
                fetches += prefix
                fetch(prefix)
            },
            prefixSettleMillis = prefixSettleMillis
        )

        val shownNames get() = model.data?.map { it.name }
        val requestedNames get() = registry.requested.toList().sorted()

        fun shownWithDetails() = model.data.orEmpty().filter { it.type == KafkaRegistryFormat.AVRO }.map { it.name }
    }

    private fun eventually(condition: () -> Boolean) = runBlocking {
        withTimeout(5_000) { while (!condition()) delay(5) }
    }

    // What the list still has going on: a wait to ask the registry, details that are loaded.
    private fun activeJobs() = scope.coroutineContext[Job]!!.children.count { it.isActive }

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

        @Test
        fun `should leave out the schemas the settings hide and tell how many`() {
            val fixture = Fixture(names = listOf("_internal-value", "orders-value", "stranger-value"))
            fixture.settings = fixture.settings.copy(showInternal = false, visibleTopics = setOf("orders"))

            val (shown, hasMore) = fixture.list.view(fixture.listing!!)

            assertEquals(listOf("orders-value"), shown.map { it.name })
            assertFalse(hasMore)
            assertEquals(HiddenSchemas(internal = 1, otherTopics = 1), fixture.list.hidden)
        }

        @Test
        fun `should hide nothing until it has been told`() {
            val fixture = Fixture()

            assertEquals(HiddenSchemas.NONE, fixture.list.hidden)
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

        @Test
        fun `should tell what the settings hide when it shows the list`() {
            val fixture = Fixture(names = listOf("_a-value", "_b-value", "orders-value"))
            fixture.settings = fixture.settings.copy(showInternal = false)

            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("orders-value") }
            assertEquals(HiddenSchemas(internal = 2, otherTopics = 0), fixture.list.hidden)

            fixture.settings = fixture.settings.copy(showInternal = true)
            fixture.list.refilter()

            eventually { fixture.shownNames?.size == 3 }
            assertEquals(HiddenSchemas.NONE, fixture.list.hidden)
        }

        @Test
        fun `should prepare before it shows the list`() = runBlocking<Unit> {
            val gate = CompletableDeferred<Unit>()
            val fixture = Fixture(prepare = { gate.await() })

            fixture.list.refilter()
            delay(100)

            assertNull(fixture.model.data)

            gate.complete(Unit)
            eventually { fixture.shownNames?.size == 10 }
        }

        @Test
        fun `should show the list with the settings as they are after it prepared`() {
            lateinit var fixture: Fixture
            fixture = Fixture(
                names = listOf("orders-value", "stranger-value"),
                prepare = { fixture.settings = fixture.settings.copy(visibleTopics = setOf("orders")) }
            )

            fixture.list.refilter()

            eventually { fixture.shownNames == listOf("orders-value") }
            assertEquals(1, fixture.list.hidden.otherTopics)
        }

        @Test
        fun `should show the list also when it can not prepare`() {
            val fixture = Fixture(prepare = { error("topics unavailable") })

            fixture.list.refilter()

            eventually { fixture.shownNames?.size == 10 }
        }

        @Test
        fun `should not show the list when it is cancelled while it prepares`() = runBlocking<Unit> {
            val fixture = Fixture(prepare = { throw CancellationException("gone") })

            fixture.list.refilter()
            delay(150)

            assertNull(fixture.model.data)
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

    @Nested
    inner class `Prefix filter` {

        private val registry = listOf(
            "orders-key", "orders-value", "ordinary-value", "payments-key", "payments-value", "subject-01", "Orders-value"
        )

        private fun listingOf(prefix: String?, loadedAt: Long = 0): SchemaListing =
            SchemaListing(
                registry.filter { prefix == null || it.startsWith(prefix) }.sortedBy { it.lowercase() }.map { KafkaSchemaInfo(it) },
                prefix,
                loadedAt
            )

        /** A fixture whose registry lists by prefix, and whose list remembers what it listed like the data manager does. */
        private fun fixtureOfPrefixes(prefixSettleMillis: Long = 0, hasFullListing: Boolean = false): Fixture =
            Fixture(prefixSettleMillis = prefixSettleMillis).also { fixture ->
                if (!hasFullListing) fixture.listing = null
                fixture.fetch = { prefix -> listingOf(prefix).also { fixture.prefixListings += it } }
            }

        private fun Fixture.filter(text: String?) {
            settings = settings.copy(filter = text)
            list.refilter()
        }

        private fun settle() = runBlocking { delay(250) }

        @Test
        fun `should ask the registry for the schemas that start with the prefix`() {
            val fixture = fixtureOfPrefixes()

            fixture.filter("^orders")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            assertEquals(listOf("orders"), fixture.fetches.toList())
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should take the case of the prefix as it is`() {
            val fixture = fixtureOfPrefixes()

            fixture.filter("^Orders")

            eventually { fixture.shownNames == listOf("Orders-value") }
        }

        @Test
        fun `should show nothing for a prefix that no schema has`() {
            val fixture = fixtureOfPrefixes()

            fixture.filter("^nothing")

            eventually { fixture.shownNames == emptyList<String>() }
            assertEquals(listOf("nothing"), fixture.fetches.toList())
        }

        @Test
        fun `should not ask the registry for a prefix when all the schemas are there`() {
            val fixture = Fixture()

            fixture.filter("^subject-0")

            eventually { fixture.shownNames == (1..9).map { "subject-%02d".format(it) } }
            assertEquals(emptyList<String>(), fixture.fetches.toList())
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should serve a longer prefix out of the listing of a shorter one`() {
            val fixture = fixtureOfPrefixes()
            fixture.filter("^ord")
            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }

            fixture.filter("^orders")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            fixture.filter("^orders-v")
            eventually { fixture.shownNames == listOf("orders-value") }
            assertEquals(listOf("ord"), fixture.fetches.toList())
        }

        @Test
        fun `should ask again for a shorter prefix`() {
            val fixture = fixtureOfPrefixes()
            fixture.filter("^orders")
            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }

            fixture.filter("^or")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }
            assertEquals(listOf("orders", "or"), fixture.fetches.toList())
        }

        @Test
        fun `should ask once the typing pauses and not for every key`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 300)

            "^orders".indices.forEach { length ->
                fixture.filter("^orders".substring(0, length + 1))
                Thread.sleep(20)
            }

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            assertEquals(listOf("orders"), fixture.fetches.toList())
        }

        @Test
        fun `should not ask for a prefix that was typed and changed again`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 200)

            fixture.filter("^pay")
            Thread.sleep(50)
            fixture.filter("^ord")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }
            Thread.sleep(300)
            assertEquals(listOf("ord"), fixture.fetches.toList())
        }

        @Test
        fun `should wait for the typing to pause again after every key`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 1_000)

            fixture.filter("^or")
            Thread.sleep(600)
            fixture.filter("^ord")
            // The wait that began with the first key is over by now, the one that began with the last key is not.
            Thread.sleep(600)

            assertEquals(emptyList<String>(), fixture.fetches.toList())
            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }
            assertEquals(listOf("ord"), fixture.fetches.toList())
        }

        @Test
        fun `should not ask for anything when the filter went back to a caret alone while it waited`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 200)

            fixture.filter("^pay")
            Thread.sleep(50)
            fixture.filter("^")
            Thread.sleep(500)

            assertEquals(emptyList<String>(), fixture.fetches.toList())
            assertNull(fixture.model.data)
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should not ask for anything when the filter is no prefix any more when the wait is over`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 200)

            fixture.filter("^pay")
            // Nobody asks the list to follow the change, so the wait is not replaced.
            fixture.settings = fixture.settings.copy(filter = "pay")
            Thread.sleep(500)

            assertEquals(emptyList<String>(), fixture.fetches.toList())
        }

        @Test
        fun `should drop the wait for a prefix when a listing serves the filter that comes next`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 60_000)
            fixture.prefixListings += listingOf("ord")
            fixture.filter("^pay")
            eventually { activeJobs() == 1 }

            fixture.filter("^ord")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }
            eventually { activeJobs() == 0 }
        }

        @Test
        fun `should not show what was listed for the prefix that was typed before`() = runBlocking<Unit> {
            val release = CompletableDeferred<Unit>()
            val fixture = fixtureOfPrefixes()
            // The registry takes its time, and does not stop for a list that is not wanted any more.
            fixture.fetch = { prefix ->
                if (prefix == "ord") withContext(NonCancellable) { release.await() }
                listingOf(prefix).also { fixture.prefixListings += it }
            }
            fixture.filter("^ord")
            eventually { fixture.fetches.toList() == listOf("ord") }

            fixture.filter("^pay")
            eventually { fixture.shownNames == listOf("payments-key", "payments-value") }
            release.complete(Unit)
            delay(150)

            assertEquals(listOf("payments-key", "payments-value"), fixture.shownNames)
        }

        @Test
        fun `should not show what was listed for a prefix when the filter asks for something else by then`() =
            runBlocking<Unit> {
                val release = CompletableDeferred<Unit>()
                val fixture = fixtureOfPrefixes()
                fixture.fetch = { prefix ->
                    release.await()
                    listingOf(prefix).also { fixture.prefixListings += it }
                }
                fixture.filter("^ord")
                eventually { fixture.fetches.toList() == listOf("ord") }

                // The filter changes, and nobody asks the list to follow it yet, so the request is not replaced.
                fixture.settings = fixture.settings.copy(filter = "^pay")
                release.complete(Unit)

                eventually { fixture.prefixListings.size == 1 }
                delay(150)
                // What was listed for "ord" has not got what "pay" asks for.
                assertNull(fixture.model.data)
                assertEquals(0, fixture.refreshes.get())
            }

        @Test
        fun `should show what was listed for the prefix that was typed before when the filter still asks for it`() =
            runBlocking<Unit> {
                val release = CompletableDeferred<Unit>()
                val fixture = fixtureOfPrefixes()
                fixture.fetch = { prefix ->
                    release.await()
                    listingOf(prefix).also { fixture.prefixListings += it }
                }
                fixture.filter("^ord")
                eventually { fixture.fetches.toList() == listOf("ord") }

                // The prefix gets longer, so what is listed for "ord" has all that the filter asks for.
                fixture.settings = fixture.settings.copy(filter = "^orders")
                release.complete(Unit)

                eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            }

        @Test
        fun `should not wait for the registry when a listing that has the schemas is there by then`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 300)
            fixture.filter("^orders")
            Thread.sleep(50)

            fixture.prefixListings += listingOf("ord")
            fixture.filter("^orders")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            Thread.sleep(400)
            assertEquals(emptyList<String>(), fixture.fetches.toList())
        }

        @Test
        fun `should use what was listed meanwhile when the wait is over`() {
            val fixture = fixtureOfPrefixes(prefixSettleMillis = 200)
            fixture.filter("^orders")

            // What a request that is still running for a shorter prefix brings.
            fixture.prefixListings += listingOf("ord")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            assertEquals(emptyList<String>(), fixture.fetches.toList())
        }

        @Test
        fun `should show the complete listing when the registry can not list by prefix`() {
            val fixture = fixtureOfPrefixes()
            // What the data manager gives then: the listing of all schemas.
            fixture.fetch = { listingOf(null) }

            fixture.filter("^ord")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value", "ordinary-value") }
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should let the model list the registry when there is no way to list by prefix`() {
            val fixture = fixtureOfPrefixes()
            fixture.fetch = { null }

            fixture.filter("^ord")

            eventually { fixture.refreshes.get() == 1 }
        }

        @Test
        fun `should let the model list the registry when the request for the prefix failed`() {
            val fixture = fixtureOfPrefixes()
            fixture.fetch = { error("registry unavailable") }

            fixture.filter("^ord")

            eventually { fixture.refreshes.get() == 1 }
            assertNull(fixture.model.data)
        }

        @Test
        fun `should not let the model list the registry for a request that was cancelled`() {
            val fixture = fixtureOfPrefixes()
            val started = CompletableDeferred<Unit>()
            fixture.fetch = { started.complete(Unit); CompletableDeferred<SchemaListing?>().await() }
            fixture.filter("^ord")
            runBlocking { withTimeout(5_000) { started.await() } }

            // Another filter replaces the request.
            fixture.fetch = { prefix -> listingOf(prefix).also { fixture.prefixListings += it } }
            fixture.filter("^pay")

            eventually { fixture.shownNames == listOf("payments-key", "payments-value") }
            settle()
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should keep the list as it is for a caret alone`() {
            val fixture = fixtureOfPrefixes()
            fixture.filter("^orders")
            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }

            fixture.filter("^")
            settle()

            assertEquals(listOf("orders-key", "orders-value"), fixture.shownNames)
            assertEquals(listOf("orders"), fixture.fetches.toList())
            assertEquals(0, fixture.refreshes.get())
        }

        @Test
        fun `should show everything for a caret alone when all the schemas are there`() {
            val fixture = Fixture()

            fixture.filter("^")

            eventually { fixture.shownNames?.size == 10 }
        }

        @Test
        fun `should not ask for a prefix when the filter is no prefix`() {
            val fixture = fixtureOfPrefixes()

            fixture.filter("orders")
            eventually { fixture.refreshes.get() == 1 }

            assertEquals(emptyList<String>(), fixture.fetches.toList())
        }

        @Test
        fun `should show all schemas that contain the text again after a prefix`() {
            val fixture = Fixture()
            fixture.filter("^subject-0")
            eventually { fixture.shownNames?.size == 9 }

            fixture.filter("-10")

            eventually { fixture.shownNames == listOf("subject-10") }
            assertEquals(emptyList<String>(), fixture.fetches.toList())
        }

        @Test
        fun `should not use a listing of a prefix for a filter that is no prefix`() {
            val fixture = fixtureOfPrefixes()
            fixture.filter("^orders")
            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }

            fixture.filter("value")

            // Only a listing of all schemas can tell which schemas contain a text.
            eventually { fixture.refreshes.get() == 1 }
            assertEquals(listOf("orders-key", "orders-value"), fixture.shownNames)
        }

        @Test
        fun `should load the details of the schemas it fetched`() {
            val fixture = fixtureOfPrefixes()

            fixture.filter("^orders")

            eventually { fixture.shownWithDetails() == listOf("orders-key", "orders-value") }
            assertEquals(listOf("orders-key", "orders-value"), fixture.requestedNames)
        }

        @Test
        fun `should leave out the internal schemas it fetched and count them`() {
            val fixture = fixtureOfPrefixes()
            fixture.fetch = { prefix ->
                SchemaListing(listOf("_a-value", "_b-value").map { KafkaSchemaInfo(it) }, prefix)
                    .also { fixture.prefixListings += it }
            }
            fixture.settings = fixture.settings.copy(showInternal = false)

            fixture.filter("^_")

            eventually { fixture.shownNames == emptyList<String>() }
            assertEquals(HiddenSchemas(internal = 2, otherTopics = 0), fixture.list.hidden)
        }

        @Test
        fun `should leave out the schemas of other topics it fetched and count them`() {
            val fixture = fixtureOfPrefixes()
            fixture.settings = fixture.settings.copy(visibleTopics = setOf("orders"))

            fixture.filter("^o")

            eventually { fixture.shownNames == listOf("orders-key", "orders-value") }
            assertEquals(HiddenSchemas(internal = 0, otherTopics = 1), fixture.list.hidden)
        }

        @Test
        fun `should keep the favorite mark of the schemas it fetched`() {
            val fixture = fixtureOfPrefixes()
            fixture.settings = fixture.settings.copy(pinned = setOf("orders-value"))

            fixture.filter("^orders")

            eventually { fixture.shownNames == listOf("orders-value", "orders-key") }
            assertTrue(fixture.model.data!!.first().isFavorite)
        }

        @Test
        fun `should show the schemas the limit leaves out as more`() {
            val fixture = fixtureOfPrefixes()
            fixture.settings = fixture.settings.copy(limit = 1)

            fixture.filter("^orders")

            eventually { fixture.shownNames == listOf("orders-key") }
            assertTrue(fixture.model.loadMore)
        }
    }

    @Nested
    inner class `View of a prefix filter` {

        private fun listingOf(prefix: String?, vararg names: String) =
            SchemaListing(names.map { KafkaSchemaInfo(it) }, prefix)

        @Test
        fun `should make the list out of a listing that has all the schemas for the prefix`() {
            val fixture = Fixture()
            fixture.settings = fixture.settings.copy(filter = "^orders-v")

            val (shown, _) = fixture.list.view(listingOf("ord", "orders-key", "orders-value", "ordinary"))

            assertEquals(listOf("orders-value"), shown.map { it.name })
        }

        @Test
        fun `should take a listing that serves the filter now if the one it was given does not`() {
            val fixture = Fixture()
            val serving = listingOf(null, "payments-key", "payments-value", "orders-value")
            fixture.cached = { prefix -> serving.takeIf { it.covers(prefix) } }
            // The registry was asked for "ord" while the filter was changed to ask for "pay".
            fixture.settings = fixture.settings.copy(filter = "^pay")

            val (shown, _) = fixture.list.view(listingOf("ord", "orders-key", "orders-value"))

            assertEquals(listOf("payments-key", "payments-value"), shown.map { it.name })
        }

        @Test
        fun `should make the list out of the listing it was given when nothing serves the filter better`() {
            val fixture = Fixture()
            fixture.cached = { null }
            fixture.settings = fixture.settings.copy(filter = "^pay")

            val (shown, _) = fixture.list.view(listingOf("ord", "orders-key", "payments-key"))

            // Better than nothing: the change of the filter brings the list it asks for.
            assertEquals(listOf("payments-key"), shown.map { it.name })
        }
    }
}
