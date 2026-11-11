package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
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
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

class PrefixListingsTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val now = AtomicLong(1_000)
    private val asked = CopyOnWriteArrayList<String>()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private val registry = listOf("ord-a", "orders-key", "orders-value", "orders2-value", "payments-value")

    private fun listingFor(prefix: String) =
        SchemaListing(registry.filter { it.startsWith(prefix) }.map { KafkaSchemaInfo(it) }, prefix, now.get())

    private fun listingsOf(
        capacity: Int = 100,
        load: suspend (String) -> SchemaListing? = { listingFor(it) }
    ) = PrefixListings(scope, ttlMillis = 1_000, capacity = capacity, clock = now::get) { prefix ->
        asked += prefix
        load(prefix)
    }

    @Test
    fun `should list the schemas that start with the prefix`() = runBlocking<Unit> {
        val listings = listingsOf()

        val listing = listings.get("orders")!!

        assertEquals(listOf("orders-key", "orders-value", "orders2-value"), listing.names)
        assertEquals("orders", listing.prefix)
        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should ask once for a prefix that is asked for again`() = runBlocking<Unit> {
        val listings = listingsOf()

        val first = listings.get("orders")!!
        val second = listings.get("orders")!!

        assertSame(first, second)
        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should ask once for callers that ask at the same time`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val listings = listingsOf { release.await(); listingFor(it) }

        val callers = (1..5).map { async(Dispatchers.Default) { listings.get("orders") } }
        release.complete(Unit)

        val results = withTimeout(5_000) { callers.awaitAll() }
        assertEquals(1, results.map { System.identityHashCode(it) }.distinct().size)
        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should serve a longer prefix out of the listing of a shorter one`() = runBlocking<Unit> {
        val listings = listingsOf()
        val shorter = listings.get("ord")!!

        assertSame(shorter, listings.get("orders"))
        assertSame(shorter, listings.get("orders-value"))

        assertEquals(listOf("ord"), asked)
    }

    @Test
    fun `should ask again for a shorter prefix or another one`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")

        listings.get("or")
        listings.get("payments")

        assertEquals(listOf("orders", "or", "payments"), asked)
    }

    @Test
    fun `should serve what was listed for a shorter prefix later out of the listing of that one`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")
        now.addAndGet(10)
        val shorter = listings.get("or")!!

        // "or" has all the schemas "orders" has, and was listed later.
        assertSame(shorter, listings.get("orders"))
        assertEquals(listOf("orders", "or"), asked)
    }

    @Test
    fun `should ask again once the listing is too old`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")

        now.addAndGet(999)
        listings.get("orders")
        assertEquals(listOf("orders"), asked)

        now.addAndGet(1)
        listings.get("orders")
        assertEquals(listOf("orders", "orders"), asked)
    }

    @Test
    fun `should not serve a longer prefix out of a listing that is too old`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("ord")

        now.addAndGet(1_000)
        listings.get("orders")

        assertEquals(listOf("ord", "orders"), asked)
    }

    @Test
    fun `should list the registry again for a refresh, whatever it has`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("ord")

        val refreshed = listings.get("orders", refresh = true)!!

        assertEquals("orders", refreshed.prefix)
        assertEquals(listOf("ord", "orders"), asked)
    }

    @Test
    fun `should list the registry again for a refresh of a prefix that was listed`() = runBlocking<Unit> {
        val listings = listingsOf()
        val first = listings.get("orders")!!
        now.addAndGet(10)

        val refreshed = listings.get("orders", refresh = true)!!

        assertEquals(listOf("orders", "orders"), asked)
        assertEquals(first.names, refreshed.names)
        assertTrue(refreshed.loadedAt > first.loadedAt)
    }

    @Test
    fun `should serve what a refresh brought afterwards`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")
        now.addAndGet(10)
        val refreshed = listings.get("orders", refresh = true)!!

        assertSame(refreshed, listings.get("orders"))
        assertEquals(listOf("orders", "orders"), asked)
    }

    @Test
    fun `should tell what was listed lately`() = runBlocking<Unit> {
        val listings = listingsOf()
        assertEquals(emptyList<SchemaListing>(), listings.cached())

        val ord = listings.get("ord")!!
        now.addAndGet(500)
        val pay = listings.get("payments")!!

        assertEquals(setOf(ord, pay), listings.cached().toSet())
        now.addAndGet(500)
        assertEquals(listOf(pay), listings.cached())
        now.addAndGet(500)
        assertEquals(emptyList<SchemaListing>(), listings.cached())
    }

    @Test
    fun `should not ask for what is cached`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")

        listings.cached()

        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should forget everything when cleared`() = runBlocking<Unit> {
        val listings = listingsOf()
        listings.get("orders")

        listings.clear()

        assertEquals(emptyList<SchemaListing>(), listings.cached())
        listings.get("orders")
        assertEquals(listOf("orders", "orders"), asked)
    }

    @Test
    fun `should not keep what a request that was running when cleared brings`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val listings = listingsOf { started.complete(Unit); release.await(); listingFor(it) }

        val beforeChange = async(Dispatchers.Default) { listings.get("orders") }
        withTimeout(5_000) { started.await() }
        listings.clear()
        release.complete(Unit)

        assertEquals("orders", withTimeout(5_000) { beforeChange.await() }!!.prefix)
        // It may have been read before the change, so nobody else gets it.
        assertEquals(emptyList<SchemaListing>(), listings.cached())
    }

    @Test
    fun `should forget the prefix that was asked for longest ago`() = runBlocking<Unit> {
        val listings = listingsOf(capacity = 2)
        listings.get("orders")
        listings.get("payments")

        // Asking for "orders" once more makes "payments" the one asked for longest ago.
        listings.get("orders", refresh = true)
        listings.get("ord-")

        assertEquals(setOf("orders", "ord-"), listings.cached().map { it.prefix }.toSet())
    }

    @Test
    fun `should not stop the request of the others when a caller gives up`() = runBlocking<Unit> {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val listings = listingsOf { started.complete(Unit); release.await(); listingFor(it) }

        val quitter = async(Dispatchers.Default) { listings.get("orders") }
        val stayer = async(Dispatchers.Default) { listings.get("orders") }
        withTimeout(5_000) { started.await() }
        quitter.cancelAndJoin()
        release.complete(Unit)

        assertEquals("orders", withTimeout(5_000) { stayer.await() }!!.prefix)
        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should fail like the registry for a request that failed, and ask again later`() = runBlocking<Unit> {
        var failures = 1
        val listings = listingsOf { if (failures-- > 0) error("registry unavailable") else listingFor(it) }

        assertThrows<IllegalStateException> { listings.get("orders") }

        assertEquals("orders", listings.get("orders")!!.prefix)
        assertEquals(listOf("orders", "orders"), asked)
    }

    @Test
    fun `should give nothing for a registry that can not list by prefix, and not ask again`() = runBlocking<Unit> {
        val listings = listingsOf { null }

        assertNull(listings.get("orders"))
        assertNull(listings.get("orders"))
        assertNull(listings.get("payments"))
        assertNull(listings.get("orders", refresh = true))

        assertEquals(listOf("orders"), asked)
    }

    @Test
    fun `should still not ask a registry that can not list by prefix after it was cleared`() = runBlocking<Unit> {
        val listings = listingsOf { null }
        listings.get("orders")

        listings.clear()

        assertNull(listings.get("orders"))
        assertEquals(listOf("orders"), asked)
    }
}
