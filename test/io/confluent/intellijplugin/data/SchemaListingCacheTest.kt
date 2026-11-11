package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

class SchemaListingCacheTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val now = AtomicLong(1_000)
    private val allAsked = CopyOnWriteArrayList<String>()
    private val prefixesAsked = CopyOnWriteArrayList<String>()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private val registry = CopyOnWriteArrayList(listOf("ord-a", "orders-key", "orders-value", "orders2-value", "payments-value"))

    private fun listedAll() = SchemaListing(registry.map { KafkaSchemaInfo(it) }, loadedAt = now.get())

    private fun listedFor(prefix: String) =
        SchemaListing(registry.filter { it.startsWith(prefix) }.map { KafkaSchemaInfo(it) }, prefix, now.get())

    private fun cacheOf(prefixListingSupported: Boolean = true, prefixFailure: Exception? = null): SchemaListingCache {
        val all = ExpiringSingleFlight(scope, ttlMillis = 1_000, clock = now::get) {
            allAsked += "all"
            listedAll()
        }
        val prefixes = PrefixListings(scope, ttlMillis = 1_000, clock = now::get) { prefix ->
            prefixesAsked += prefix
            prefixFailure?.let { throw it }
            if (prefixListingSupported) listedFor(prefix) else null
        }
        return SchemaListingCache(all, prefixes)
    }

    @Nested
    inner class ListingFor {

        @Test
        fun `should list all the schemas for a filter that is no prefix`() = runBlocking<Unit> {
            val cache = cacheOf()

            val listing = cache.listingFor(prefix = null, refresh = false)

            assertNull(listing.prefix)
            assertEquals(registry, listing.names)
            assertEquals(listOf("all"), allAsked)
            assertTrue(prefixesAsked.isEmpty())
        }

        @Test
        fun `should list all the schemas for a prefix that is still being typed`() = runBlocking<Unit> {
            val cache = cacheOf()

            val listing = cache.listingFor(prefix = "", refresh = false)

            assertNull(listing.prefix)
            assertEquals(listOf("all"), allAsked)
            assertTrue(prefixesAsked.isEmpty())
        }

        @Test
        fun `should list only the schemas of a prefix`() = runBlocking<Unit> {
            val cache = cacheOf()

            val listing = cache.listingFor(prefix = "orders", refresh = false)

            assertEquals("orders", listing.prefix)
            assertEquals(listOf("orders-key", "orders-value", "orders2-value"), listing.names)
            assertEquals(listOf("orders"), prefixesAsked)
            assertTrue(allAsked.isEmpty())
        }

        @Test
        fun `should take the listing of all schemas that is there instead of asking for a prefix`() = runBlocking<Unit> {
            val cache = cacheOf()
            val all = cache.listingFor(prefix = null, refresh = false)

            val listing = cache.listingFor(prefix = "orders", refresh = false)

            assertSame(all, listing)
            assertTrue(prefixesAsked.isEmpty())
        }

        @Test
        fun `should ask for a prefix again when told to refresh even if all the schemas are listed`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)

            val listing = cache.listingFor(prefix = "orders", refresh = true)

            assertEquals("orders", listing.prefix)
            assertEquals(listOf("orders"), prefixesAsked)
            assertEquals(listOf("all"), allAsked)
        }

        @Test
        fun `should list again only the prefix when told to refresh`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = "orders", refresh = false)

            cache.listingFor(prefix = "orders", refresh = true)

            assertEquals(listOf("orders", "orders"), prefixesAsked)
            assertTrue(allAsked.isEmpty())
        }

        @Test
        fun `should list all the schemas again when told to refresh without a prefix`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)

            cache.listingFor(prefix = null, refresh = true)

            assertEquals(listOf("all", "all"), allAsked)
        }

        @Test
        fun `should list all the schemas when the registry can not list by prefix`() = runBlocking<Unit> {
            val cache = cacheOf(prefixListingSupported = false)

            val listing = cache.listingFor(prefix = "orders", refresh = false)

            assertNull(listing.prefix)
            assertEquals(registry, listing.names)
            assertEquals(listOf("orders"), prefixesAsked)
            assertEquals(listOf("all"), allAsked)
        }

        @Test
        fun `should not ask a registry that can not list by prefix again`() = runBlocking<Unit> {
            val cache = cacheOf(prefixListingSupported = false)
            cache.listingFor(prefix = "orders", refresh = false)

            cache.listingFor(prefix = "payments", refresh = false)

            assertEquals(listOf("orders"), prefixesAsked)
        }
    }

    @Nested
    inner class CachedFor {

        @Test
        fun `should have nothing before anything was listed`() {
            val cache = cacheOf()

            assertNull(cache.cachedFor(null))
            assertNull(cache.cachedFor("orders"))
        }

        @Test
        fun `should give the listing of all schemas for any filter`() = runBlocking<Unit> {
            val cache = cacheOf()
            val all = cache.listingFor(prefix = null, refresh = false)

            assertSame(all, cache.cachedFor(null))
            assertSame(all, cache.cachedFor("orders"))
        }

        @Test
        fun `should give a listing of a prefix only for a filter with that prefix or a longer one`() = runBlocking<Unit> {
            val cache = cacheOf()
            val listing = cache.listingFor(prefix = "ord", refresh = false)

            assertSame(listing, cache.cachedFor("ord"))
            assertSame(listing, cache.cachedFor("orders"))
            assertNull(cache.cachedFor("pay"))
            assertNull(cache.cachedFor(null))
        }

        @Test
        fun `should not give a listing that is too old`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = "orders", refresh = false)

            now.addAndGet(1_000)

            assertNull(cache.cachedFor("orders"))
        }
    }

    @Nested
    inner class KnownToHave {

        @Test
        fun `should not know anything before anything was listed`() {
            val cache = cacheOf()

            assertNull(cache.knownToHave("orders-value"))
        }

        @Test
        fun `should know both ways from the listing of all schemas`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)

            assertEquals(true, cache.knownToHave("orders-value"))
            assertEquals(false, cache.knownToHave("nothing-value"))
        }

        @Test
        fun `should know both ways for what a prefix listing covers`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = "orders", refresh = false)

            assertEquals(true, cache.knownToHave("orders-value"))
            assertEquals(false, cache.knownToHave("orders-missing"))
        }

        @Test
        fun `should not know what a prefix listing does not cover`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = "orders", refresh = false)

            assertNull(cache.knownToHave("payments-value"))
        }
    }

    @Nested
    inner class Has {

        @Test
        fun `should answer from what was listed without asking`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)
            allAsked.clear()

            assertTrue(cache.has("orders-value"))
            assertFalse(cache.has("nothing-value"))
            assertTrue(allAsked.isEmpty())
            assertTrue(prefixesAsked.isEmpty())
        }

        @Test
        fun `should ask for the schemas that start with the name if nothing was listed`() = runBlocking<Unit> {
            val cache = cacheOf()

            assertTrue(cache.has("orders-value"))

            assertEquals(listOf("orders-value"), prefixesAsked)
            assertTrue(allAsked.isEmpty())
        }

        @Test
        fun `should find out that a schema is missing`() = runBlocking<Unit> {
            val cache = cacheOf()

            assertFalse(cache.has("nothing-value"))
        }

        @Test
        fun `should list all the schemas if the registry can not list by prefix`() = runBlocking<Unit> {
            val cache = cacheOf(prefixListingSupported = false)

            assertTrue(cache.has("orders-value"))
            assertFalse(cache.has("nothing-value"))

            assertEquals(listOf("all"), allAsked)
        }
    }

    @Nested
    inner class Listed {

        @Test
        fun `should list everything that was listed`() = runBlocking<Unit> {
            val cache = cacheOf()
            val all = cache.listingFor(prefix = null, refresh = false)
            val orders = cache.listingFor(prefix = "orders", refresh = true)

            assertEquals(setOf(all, orders), cache.listed().toSet())
        }
    }

    @Nested
    inner class Clear {

        @Test
        fun `should forget all and the prefixes`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)
            cache.listingFor(prefix = "orders", refresh = true)

            cache.clear()

            assertTrue(cache.listed().isEmpty())
            assertNull(cache.cachedFor(null))
            assertNull(cache.knownToHave("orders-value"))
        }

        @Test
        fun `should list again after it was cleared`() = runBlocking<Unit> {
            val cache = cacheOf()
            cache.listingFor(prefix = null, refresh = false)

            cache.clear()
            cache.listingFor(prefix = null, refresh = false)

            assertEquals(listOf("all", "all"), allAsked)
        }

        @Test
        fun `should keep that the registry can not list by prefix`() = runBlocking<Unit> {
            val cache = cacheOf(prefixListingSupported = false)
            cache.listingFor(prefix = "orders", refresh = false)

            cache.clear()
            cache.listingFor(prefix = "payments", refresh = false)

            assertEquals(listOf("orders"), prefixesAsked)
        }
    }
}
