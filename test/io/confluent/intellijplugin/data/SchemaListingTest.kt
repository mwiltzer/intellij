package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
}
