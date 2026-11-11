package io.confluent.intellijplugin.data

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class SchemaNamesFlightTest {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val now = AtomicLong(1_000)
    private val listings = AtomicInteger()

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    // A registry that lists by prefix if it can, which is only known once it was asked.
    private fun prefixesOf(canListByPrefix: Boolean) = PrefixListings(scope, clock = now::get) { prefix ->
        if (canListByPrefix) SchemaListing(emptyList(), prefix, now.get()) else null
    }

    private fun flightOf(prefixes: PrefixListings) = schemaNamesFlight(scope, prefixes, now::get) {
        listings.incrementAndGet()
        SchemaListing(emptyList(), loadedAt = now.get())
    }

    @Test
    fun `should keep the names for a minute as long as the registry was not asked to list by prefix`() = runBlocking<Unit> {
        val flight = flightOf(prefixesOf(canListByPrefix = true))
        flight.get()

        now.addAndGet(SCHEMA_NAMES_SHORT_TTL_MS - 1)
        assertNotNull(flight.peekFresh())

        now.addAndGet(1)
        assertNull(flight.peekFresh())
    }

    @Test
    fun `should keep the names for five minutes once the registry listed by prefix`() = runBlocking<Unit> {
        val prefixes = prefixesOf(canListByPrefix = true)
        val flight = flightOf(prefixes)
        flight.get()
        prefixes.get("orders")

        now.addAndGet(SCHEMA_NAMES_TTL_MS - 1)
        assertNotNull(flight.peekFresh())
        assertEquals(1, listings.get())

        now.addAndGet(1)
        assertNull(flight.peekFresh())
        flight.get()
        assertEquals(2, listings.get())
    }

    @Test
    fun `should keep the names that were listed before the registry listed by prefix for as long as it says now`() =
        runBlocking<Unit> {
            val prefixes = prefixesOf(canListByPrefix = true)
            val flight = flightOf(prefixes)
            flight.get()
            now.addAndGet(2 * SCHEMA_NAMES_SHORT_TTL_MS)
            assertNull(flight.peekFresh())

            prefixes.get("orders")

            assertNotNull(flight.peekFresh())
            flight.get()
            assertEquals(1, listings.get())
        }

    @Test
    fun `should keep the names for a minute for a registry that can not list by prefix`() = runBlocking<Unit> {
        val prefixes = prefixesOf(canListByPrefix = false)
        val flight = flightOf(prefixes)
        flight.get()
        prefixes.get("orders")

        now.addAndGet(SCHEMA_NAMES_SHORT_TTL_MS - 1)
        assertNotNull(flight.peekFresh())

        now.addAndGet(1)
        assertNull(flight.peekFresh())
    }
}
