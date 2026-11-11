package io.confluent.intellijplugin.consumer.models

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.regex.PatternSyntaxException

/** The filter of the consumer matches the text the table shows, and still what it matched before. */
@TestApplication
class ConsumerFilterShownTextTest {

    private fun record(key: Any? = "key", value: Any? = "value"): ConsumerRecord<Any, Any> =
        ConsumerRecord(
            "topic", 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0, key, value, RecordHeaders(),
            java.util.Optional.empty()
        )

    private fun filter(
        type: ConsumerFilterType,
        key: String? = null,
        value: String? = null
    ) = ConsumerFilter(
        filterKey = key,
        filterValue = value,
        filterHeadKey = null,
        filterHeadValue = null,
        type = type
    )

    private fun field(type: KafkaFieldType, format: KafkaRegistryFormat = KafkaRegistryFormat.UNKNOWN) =
        ConsumerProducerFieldConfig(
            type = type,
            valueText = "",
            isKey = false,
            topic = "topic",
            registryType = KafkaRegistryType.CONFLUENT,
            schemaName = "subject",
            schemaFormat = format,
            parsedSchema = null
        )

    /** A value whose plain text differs from the text it is shown with, like the objects of a schema do. */
    private class Shown(private val plain: String) {
        override fun toString() = plain
    }

    /** A value that cannot be turned into json, as reading one of its properties fails. */
    class Opaque {
        @Suppress("unused")
        val broken: String get() = error("This value cannot be read")

        override fun toString() = "plain"
    }

    @Nested
    @DisplayName("Shown text")
    inner class ShownText {

        private val shown: (Any) -> String = { "shown:$it" }

        @Test
        fun `should pass on the shown text`() {
            val filter = filter(ConsumerFilterType.CONTAINS, value = "shown:plain")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should still pass on the plain text`() {
            val filter = filter(ConsumerFilterType.CONTAINS, value = "plai")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should not pass when neither of the texts contains the filter`() {
            val filter = filter(ConsumerFilterType.CONTAINS, value = "other")

            assertFalse(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should use the shown text of the key for the key and of the value for the value`() {
            val filter = filter(ConsumerFilterType.CONTAINS, key = "shown-key:k", value = "shown-value:v")
            val keyText: (Any) -> String = { "shown-key:$it" }
            val valueText: (Any) -> String = { "shown-value:$it" }

            assertTrue(filter.isRecordPassFilter(record(key = "k", value = "v"), keyText, valueText))
            assertFalse(filter.isRecordPassFilter(record(key = "k", value = "v"), valueText, keyText))
        }

        @Test
        fun `should match a pattern against the shown text`() {
            val filter = filter(ConsumerFilterType.REGEX, value = "shown:p.*")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should still match a pattern against the plain text`() {
            val filter = filter(ConsumerFilterType.REGEX, value = "pl.*")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should not pass a does not contain filter when the shown text contains it`() {
            val filter = filter(ConsumerFilterType.DOES_NOT_CONTAINS, value = "shown:")

            assertFalse(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should not pass a does not contain filter when the plain text contains it`() {
            val filter = filter(ConsumerFilterType.DOES_NOT_CONTAINS, value = "plai")

            assertFalse(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should pass a does not contain filter when none of the texts contains it`() {
            val filter = filter(ConsumerFilterType.DOES_NOT_CONTAINS, value = "other")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), valueText = shown))
        }

        @Test
        fun `should not pass a missing key or value whatever the filter type is`() {
            val typesWithText = ConsumerFilterType.entries.filter { it != ConsumerFilterType.NONE }

            typesWithText.forEach {
                assertFalse(filter(it, key = ".*").isRecordPassFilter(record(key = null), shown, shown), "$it key")
                assertFalse(filter(it, value = ".*").isRecordPassFilter(record(value = null), shown, shown), "$it value")
            }
        }
    }

    @Nested
    @DisplayName("Laziness")
    inner class Laziness {

        private var keyCalls = 0
        private var valueCalls = 0
        private val keyText: (Any) -> String = { keyCalls++; "shown:$it" }
        private val valueText: (Any) -> String = { valueCalls++; "shown:$it" }

        @Test
        fun `should not make the text of a key or value that has no filter`() {
            val filter = filter(ConsumerFilterType.CONTAINS, key = "shown:a")

            assertTrue(filter.isRecordPassFilter(record(key = Shown("a")), keyText, valueText))

            assertEquals(1, keyCalls)
            assertEquals(0, valueCalls)
        }

        @Test
        fun `should not make any text when the filter type is none`() {
            val filter = filter(ConsumerFilterType.NONE, key = "x", value = "y")

            assertTrue(filter.isRecordPassFilter(record(), keyText, valueText))

            assertEquals(0, keyCalls)
            assertEquals(0, valueCalls)
        }

        @Test
        fun `should not make the shown text when the plain text decided already`() {
            val filter = filter(ConsumerFilterType.CONTAINS, value = "plai")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), keyText, valueText))

            assertEquals(0, valueCalls)
        }

        @Test
        fun `should not make the text of a missing value`() {
            val filter = filter(ConsumerFilterType.CONTAINS, value = "x")

            assertFalse(filter.isRecordPassFilter(record(value = null), keyText, valueText))

            assertEquals(0, valueCalls)
        }

        @Test
        fun `should make the shown text once for a record`() {
            val filter = filter(ConsumerFilterType.DOES_NOT_CONTAINS, value = "other")

            assertTrue(filter.isRecordPassFilter(record(value = Shown("plain")), keyText, valueText))

            assertEquals(1, valueCalls)
        }
    }

    @Nested
    @DisplayName("Fields of the consumer")
    inner class Fields {

        private val person = SchemaBuilder.record("Person").namespace("io.example").fields()
            .requiredString("name")
            .optionalInt("age")
            .endRecord()
            .let { GenericRecordBuilder(it).set("name", "Ada").set("age", 36).build() }

        private val schemaField = field(KafkaFieldType.SCHEMA_REGISTRY, KafkaRegistryFormat.AVRO)

        private fun passes(filter: ConsumerFilter, key: Any? = "key", value: Any? = person) =
            filter.predicateFor(field(KafkaFieldType.STRING), schemaField)(record(key, value))

        @Test
        fun `should pass an avro record on the text the table shows`() {
            // What the table shows has no space after the colon and wraps the value of a union by its type.
            assertTrue(passes(filter(ConsumerFilterType.CONTAINS, value = "\"name\":\"Ada\"")))
            assertTrue(passes(filter(ConsumerFilterType.CONTAINS, value = "\"age\":{\"int\":36}")))
        }

        @Test
        fun `should still pass an avro record on the text it had before`() {
            assertTrue(passes(filter(ConsumerFilterType.CONTAINS, value = "\"name\": \"Ada\"")))
            assertTrue(passes(filter(ConsumerFilterType.CONTAINS, value = "\"age\": 36")))
        }

        @Test
        fun `should pass an avro record on its schema name, which the table shows, too`() {
            assertTrue(passes(filter(ConsumerFilterType.CONTAINS, value = "io.example.Person")))
        }

        @Test
        fun `should not pass an avro record that has none of the texts`() {
            assertFalse(passes(filter(ConsumerFilterType.CONTAINS, value = "\"name\":\"Grace\"")))
        }

        @Test
        fun `should pass a binary key on the base64 text the table shows`() {
            val key = "hello".toByteArray()
            val keyField = field(KafkaFieldType.BASE64)
            val predicate = filter(ConsumerFilterType.CONTAINS, key = "aGVsbG8").predicateFor(keyField, schemaField)

            assertTrue(predicate(record(key = key, value = person)))
        }

        @Test
        fun `should pass a json value on the compact text the table shows and on the text it was written with`() {
            val jsonField = field(KafkaFieldType.JSON)
            val written = "{\"name\": \"Ada\", \"tags\": [1, 2]}"

            fun passes(text: String) =
                filter(ConsumerFilterType.CONTAINS, value = text).predicateFor(jsonField, jsonField)(record(value = written))

            assertTrue(passes("\"name\":\"Ada\""))
            assertTrue(passes("\"name\": \"Ada\""))
            assertTrue(passes("\"tags\":[1,2]"))
            assertFalse(passes("\"name\":\"Grace\""))
        }

        @Test
        fun `should fall back to the plain text of a value that cannot be shown`() {
            // A json schema value has to be turned into json first, which fails for a value that cannot be read.
            val jsonSchemaField = field(KafkaFieldType.SCHEMA_REGISTRY, KafkaRegistryFormat.JSON)

            assertEquals("plain", jsonSchemaField.shownText(Opaque()))
        }

        @Test
        fun `should match a pattern against the avro text the table shows`() {
            assertTrue(passes(filter(ConsumerFilterType.REGEX, value = ".*\"name\":\"Ada\".*")))
        }
    }

    @Nested
    @DisplayName("Regular expression problems")
    inner class RegexProblems {

        @Test
        fun `should find no problem in a valid pattern`() {
            assertNull(ConsumerFilter.regexProblem("a(b|c)+.*"))
        }

        @Test
        fun `should find no problem in an empty pattern, which is no filter`() {
            assertNull(ConsumerFilter.regexProblem(""))
            assertNull(ConsumerFilter.regexProblem("   "))
        }

        @Test
        fun `should say what is wrong and where`() {
            val problem = ConsumerFilter.regexProblem("a(b")

            assertNotNull(problem)
            assertTrue(problem!!.startsWith("Not a valid regular expression: "), problem)
            assertTrue(problem.contains("Unclosed group"), problem)
            assertTrue(problem.contains("at character"), problem)
            assertFalse(problem.contains("\n"), "The text is shown in a tooltip: $problem")
        }

        @Test
        fun `should leave the place out when the problem has none`() {
            val problem = ConsumerFilter.describe(PatternSyntaxException("Unclosed group", "a(b", -1))

            assertEquals("Not a valid regular expression: Unclosed group", problem)
        }

        @Test
        fun `should count the place from one like a user does`() {
            val problem = ConsumerFilter.describe(PatternSyntaxException("Unclosed group", "a(b", 2))

            assertEquals("Not a valid regular expression: Unclosed group (at character 3)", problem)
        }
    }
}
