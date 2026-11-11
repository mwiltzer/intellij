package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class SchemaListingTest {

    private val listing = SchemaListing(listOf("a-value", "B-value", "c").map { KafkaSchemaInfo(it) })

    @Test
    fun `should keep the names in the order of the schemas`() {
        assertEquals(listOf("a-value", "B-value", "c"), listing.names)
    }

    @Test
    fun `should tell which schemas it has`() {
        assertTrue("a-value" in listing)
        assertTrue("B-value" in listing)
        assertTrue("c" in listing)
    }

    @Test
    fun `should take names as they are written`() {
        assertFalse("b-value" in listing)
        assertFalse("a-value " in listing)
        assertFalse("" in listing)
    }

    @Test
    fun `should have nothing when nothing was listed`() {
        assertFalse("a-value" in SchemaListing(emptyList()))
    }

    @Nested
    inner class `a listing of all schemas` {
        private val all = SchemaListing(listOf("a-value").map { KafkaSchemaInfo(it) })

        @Test
        fun `should not have a prefix`() {
            assertNull(all.prefix)
        }

        @Test
        fun `should cover every filter`() {
            assertTrue(all.covers(null))
            assertTrue(all.covers(""))
            assertTrue(all.covers("orders"))
        }

        @Test
        fun `should know about every schema`() {
            assertTrue(all.knowsAbout("a-value"))
            assertTrue(all.knowsAbout("anything"))
        }
    }

    @Nested
    inner class `a listing for a prefix` {
        private val prefixed = SchemaListing(listOf("ord-a", "orders-value").map { KafkaSchemaInfo(it) }, prefix = "ord")

        @Test
        fun `should cover the filters for the same prefix or a longer one`() {
            assertTrue(prefixed.covers("ord"))
            assertTrue(prefixed.covers("orders"))
            assertTrue(prefixed.covers("orders-value"))
        }

        @Test
        fun `should not cover the filters for a shorter prefix`() {
            assertFalse(prefixed.covers("or"))
            assertFalse(prefixed.covers("o"))
            assertFalse(prefixed.covers(""))
        }

        @Test
        fun `should not cover the filters for another prefix`() {
            assertFalse(prefixed.covers("payments"))
            assertFalse(prefixed.covers("Ord"))
            assertFalse(prefixed.covers("xord"))
        }

        @Test
        fun `should not cover a filter that is no prefix`() {
            assertFalse(prefixed.covers(null))
        }

        @Test
        fun `should know about the schemas that start with the prefix`() {
            assertTrue(prefixed.knowsAbout("ord"))
            assertTrue(prefixed.knowsAbout("orders-value"))
            assertTrue(prefixed.knowsAbout("ordinary"))
        }

        @Test
        fun `should not know about the others`() {
            assertFalse(prefixed.knowsAbout("payments-value"))
            assertFalse(prefixed.knowsAbout("or"))
            assertFalse(prefixed.knowsAbout("Orders-value"))
        }

        @Test
        fun `should not know about a schema that has the prefix elsewhere in its name`() {
            assertFalse(prefixed.knowsAbout("disorder-value"))
            assertFalse(prefixed.knowsAbout("x-ord"))
        }
    }

    @Nested
    inner class `the listing to make the list out of` {
        private fun listing(prefix: String?, loadedAt: Long) =
            SchemaListing(emptyList(), prefix = prefix, loadedAt = loadedAt)

        @Test
        fun `should give nothing without listings`() {
            assertNull(SchemaListings.bestFor("orders", emptyList()))
        }

        @Test
        fun `should leave out the listings that do not have all the schemas`() {
            val shorter = listing("or", 1)
            val other = listing("pay", 2)

            assertNull(SchemaListings.bestFor("o", listOf(shorter, other)))
            assertNull(SchemaListings.bestFor(null, listOf(shorter, other)))
        }

        @Test
        fun `should take the one that has all the schemas for a filter that is no prefix`() {
            val all = listing(null, 1)

            assertSame(all, SchemaListings.bestFor(null, listOf(listing("or", 5), all)))
        }

        @Test
        fun `should take the one that was loaded last`() {
            val old = listing(null, 1)
            val newer = listing("or", 2)
            val newest = listing("ord", 3)

            assertSame(newest, SchemaListings.bestFor("orders", listOf(old, newer, newest)))
            assertSame(newest, SchemaListings.bestFor("orders", listOf(newest, newer, old)))
            assertSame(newer, SchemaListings.bestFor("orders", listOf(old, newer)))
        }

        @Test
        fun `should take the listing of all schemas of two that were loaded at the same time`() {
            val all = listing(null, 7)
            val prefixed = listing("ord", 7)

            assertSame(all, SchemaListings.bestFor("orders", listOf(prefixed, all)))
            assertSame(all, SchemaListings.bestFor("orders", listOf(all, prefixed)))
        }

        @Test
        fun `should take the listing for the shorter prefix of two that were loaded at the same time`() {
            val shorter = listing("or", 7)
            val longer = listing("ord", 7)

            assertSame(shorter, SchemaListings.bestFor("orders", listOf(longer, shorter)))
            assertSame(shorter, SchemaListings.bestFor("orders", listOf(shorter, longer)))
        }

        @Test
        fun `should take what was loaded later even when it is less general`() {
            val general = listing(null, 1)
            val specific = listing("orders", 2)

            assertSame(specific, SchemaListings.bestFor("orders", listOf(general, specific)))
        }

        @Test
        fun `should take an old listing of all schemas when nothing else has them all`() {
            val old = listing(null, 1)

            assertSame(old, SchemaListings.bestFor("orders", listOf(old, listing("pay", 9))))
        }
    }

    @Nested
    inner class `the listing that knows about a schema` {
        private fun listing(prefix: String?, loadedAt: Long) =
            SchemaListing(emptyList(), prefix = prefix, loadedAt = loadedAt)

        @Test
        fun `should give nothing when no listing knows`() {
            assertNull(SchemaListings.knowingAbout("orders-value", listOf(listing("pay", 1))))
            assertNull(SchemaListings.knowingAbout("orders-value", emptyList()))
        }

        @Test
        fun `should take the one that was loaded last of those that know`() {
            val old = listing(null, 1)
            val newer = listing("ord", 5)
            val other = listing("pay", 9)

            assertSame(newer, SchemaListings.knowingAbout("orders-value", listOf(old, newer, other)))
            assertSame(old, SchemaListings.knowingAbout("payments-value", listOf(old, newer)))
        }

        @Test
        fun `should take the listing of all schemas of two that were loaded at the same time`() {
            val all = listing(null, 7)
            val prefixed = listing("ord", 7)

            assertSame(all, SchemaListings.knowingAbout("orders-value", listOf(prefixed, all)))
        }
    }
}
