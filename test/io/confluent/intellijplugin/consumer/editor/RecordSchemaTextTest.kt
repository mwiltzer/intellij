package io.confluent.intellijplugin.consumer.editor

import io.confluent.intellijplugin.registry.RecordSchema
import io.confluent.intellijplugin.registry.SchemaUsage
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class RecordSchemaTextTest {

    private val guid = UUID.fromString("6c9d0a1e-1111-2222-3333-444455556666")

    @Test
    fun `should show nothing for a record that says nothing`() {
        assertEquals("", RecordSchemaText.of(null))
        assertEquals("", RecordSchemaText.of(RecordSchema()))
        assertEquals("", RecordSchemaText.of(RecordSchema(), listOf(SchemaUsage("orders-value", 3))))
    }

    @Test
    fun `should show the ID alone while the subjects are not known`() {
        assertEquals("100016", RecordSchemaText.of(RecordSchema(id = 100016)))
        assertEquals("100016", RecordSchemaText.of(RecordSchema(id = 100016), emptyList()))
    }

    @Test
    fun `should show the ID without the grouping of the locale`() {
        assertEquals("1234567", RecordSchemaText.of(RecordSchema(id = 1234567)))
    }

    @Test
    fun `should show the subject and version of the schema`() {
        val text = RecordSchemaText.of(RecordSchema(id = 100016), listOf(SchemaUsage("orders-value", 3)))

        assertEquals("100016 (orders-value, version 3)", text)
    }

    @Test
    fun `should show a version without the grouping of the locale`() {
        val text = RecordSchemaText.of(RecordSchema(id = 7), listOf(SchemaUsage("orders-value", 1234)))

        assertEquals("7 (orders-value, version 1234)", text)
    }

    @Test
    fun `should show the subjects in the order of their names`() {
        val text = RecordSchemaText.of(
            RecordSchema(id = 7),
            listOf(SchemaUsage("b-value", 1), SchemaUsage("A-value", 2), SchemaUsage("a-value", 5), SchemaUsage("a-value", 4))
        )

        assertEquals("7 (A-value, version 2; a-value, version 4; a-value, version 5; +1 more)", text)
    }

    @Test
    fun `should count the subjects that are too many to show`() {
        val usages = (1..10).map { SchemaUsage("subject-%02d".format(it), it) }

        val text = RecordSchemaText.of(RecordSchema(id = 7), usages)

        assertEquals(
            "7 (subject-01, version 1; subject-02, version 2; subject-03, version 3; +7 more)",
            text
        )
    }

    @Test
    fun `should show as many subjects as it can list without counting`() {
        val usages = (1..RecordSchemaText.MAX_USAGES_SHOWN).map { SchemaUsage("subject-$it", it) }

        val text = RecordSchemaText.of(RecordSchema(id = 7), usages)

        assertEquals("7 (subject-1, version 1; subject-2, version 2; subject-3, version 3)", text)
    }

    @Test
    fun `should name the subjects of the topic first`() {
        val usages = listOf(
            SchemaUsage("a-value", 1),
            SchemaUsage("b-value", 1),
            SchemaUsage("c-value", 1),
            SchemaUsage("orders-value", 4),
        )

        val text = RecordSchemaText.of(RecordSchema(id = 7), usages, topic = "orders")

        assertEquals("7 (orders-value, version 4; a-value, version 1; b-value, version 1; +1 more)", text)
    }

    @Test
    fun `should take the key subject and the bare name of the topic as its subjects`() {
        val usages = listOf(SchemaUsage("a-value", 1), SchemaUsage("orders", 2), SchemaUsage("orders-key", 3))

        val text = RecordSchemaText.of(RecordSchema(id = 7), usages, topic = "orders")

        assertEquals("7 (orders, version 2; orders-key, version 3; a-value, version 1)", text)
    }

    @Test
    fun `should not take the subject of another topic for one of this topic`() {
        val usages = listOf(SchemaUsage("a-value", 1), SchemaUsage("orders-archive-value", 2))

        val text = RecordSchemaText.of(RecordSchema(id = 7), usages, topic = "orders")

        assertEquals("7 (a-value, version 1; orders-archive-value, version 2)", text)
    }

    @Test
    fun `should show the GUID of a schema that has no ID`() {
        assertEquals("GUID $guid", RecordSchemaText.of(RecordSchema(guid = guid)))
    }

    @Test
    fun `should show the GUID without the subjects, which are not asked for`() {
        assertEquals("GUID $guid", RecordSchemaText.of(RecordSchema(guid = guid), listOf(SchemaUsage("orders-value", 3))))
    }

    @Test
    fun `should show the ID of a schema that has both`() {
        assertEquals("7", RecordSchemaText.of(RecordSchema(id = 7, guid = guid)))
    }
}
