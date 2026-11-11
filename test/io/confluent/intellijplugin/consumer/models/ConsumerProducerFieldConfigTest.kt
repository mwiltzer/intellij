package io.confluent.intellijplugin.consumer.models

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*

@TestApplication
class ConsumerProducerFieldConfigTest {

    private fun config(
        type: KafkaFieldType,
        valueText: String,
        schemaFormat: KafkaRegistryFormat = KafkaRegistryFormat.UNKNOWN
    ) = ConsumerProducerFieldConfig(
        type = type,
        valueText = valueText,
        isKey = false,
        topic = "test-topic",
        registryType = KafkaRegistryType.NONE,
        schemaName = "",
        schemaFormat = schemaFormat,
        parsedSchema = null
    )

    @Nested
    @DisplayName("getValueObj - primitive types")
    inner class PrimitiveTypes {

        @Test
        fun `should return string value for STRING type`() {
            val result = config(KafkaFieldType.STRING, "hello").getValueObj()
            assertEquals("hello", result)
        }

        @Test
        fun `should return null for STRING type with empty string`() {
            val result = config(KafkaFieldType.STRING, "").getValueObj()
            assertNull(result)
        }

        @Test
        fun `should return raw string for JSON type`() {
            val json = """{"key": "value"}"""
            val result = config(KafkaFieldType.JSON, json).getValueObj()
            assertEquals(json, result)
        }

        @Test
        fun `should return raw string for MESSAGEPACK type`() {
            val json = """{"key": "value"}"""
            val result = config(KafkaFieldType.MESSAGEPACK, json).getValueObj()
            assertEquals(json, result)
        }

        @Test
        fun `should return Long for LONG type`() {
            val result = config(KafkaFieldType.LONG, "9876543210").getValueObj()
            assertEquals(9876543210L, result)
            assertTrue(result is Long)
        }

        @Test
        fun `should return Int for INTEGER type`() {
            val result = config(KafkaFieldType.INTEGER, "42").getValueObj()
            assertEquals(42, result)
            assertTrue(result is Int)
        }

        @Test
        fun `should return Double for DOUBLE type`() {
            val result = config(KafkaFieldType.DOUBLE, "3.14").getValueObj()
            assertEquals(3.14, result)
            assertTrue(result is Double)
        }

        @Test
        fun `should return Float for FLOAT type`() {
            val result = config(KafkaFieldType.FLOAT, "2.5").getValueObj()
            assertEquals(2.5f, result)
            assertTrue(result is Float)
        }

        @Test
        fun `should return decoded byte array for BASE64 type`() {
            val original = "Hello, World!"
            val encoded = Base64.getEncoder().encodeToString(original.toByteArray())
            val result = config(KafkaFieldType.BASE64, encoded).getValueObj()
            assertTrue(result is ByteArray)
            assertEquals(original, String(result as ByteArray))
        }

        @Test
        fun `should return null for NULL type`() {
            val result = config(KafkaFieldType.NULL, "anything").getValueObj()
            assertNull(result)
        }
    }

    @Nested
    @DisplayName("getValueObj - error cases")
    inner class ErrorCases {

        @Test
        fun `should throw NumberFormatException for LONG with non-numeric text`() {
            assertThrows<NumberFormatException> {
                config(KafkaFieldType.LONG, "not-a-number").getValueObj()
            }
        }

        @Test
        fun `should throw NumberFormatException for INTEGER with non-numeric text`() {
            assertThrows<NumberFormatException> {
                config(KafkaFieldType.INTEGER, "abc").getValueObj()
            }
        }

        @Test
        fun `should throw NumberFormatException for DOUBLE with non-numeric text`() {
            assertThrows<NumberFormatException> {
                config(KafkaFieldType.DOUBLE, "xyz").getValueObj()
            }
        }

        @Test
        fun `should throw NumberFormatException for FLOAT with non-numeric text`() {
            assertThrows<NumberFormatException> {
                config(KafkaFieldType.FLOAT, "not-float").getValueObj()
            }
        }

        @Test
        fun `should throw IllegalArgumentException for BASE64 with invalid base64`() {
            assertThrows<IllegalArgumentException> {
                config(KafkaFieldType.BASE64, "not-valid-base64!!!").getValueObj()
            }
        }
    }

    @Nested
    @DisplayName("getValueObj - SCHEMA_REGISTRY type")
    inner class SchemaRegistryType {

        @Test
        fun `should throw exception when valueText is blank for SCHEMA_REGISTRY type`() {
            assertThrows<Exception> {
                config(KafkaFieldType.SCHEMA_REGISTRY, "  ").getValueObj()
            }
        }

        @Test
        fun `should throw exception when valueText is empty for SCHEMA_REGISTRY type`() {
            assertThrows<Exception> {
                config(KafkaFieldType.SCHEMA_REGISTRY, "").getValueObj()
            }
        }
    }

    @Nested
    @DisplayName("getValueObj - Avro")
    inner class AvroValues {

        private val avroSchema = AvroSchema(
            """
            {"type":"record","name":"Order","namespace":"shop","fields":[
              {"name":"id","type":"long"},
              {"name":"customerName","type":["null","string"],"default":null},
              {"name":"createdAt","type":{"type":"long","logicalType":"timestamp-millis"}}
            ]}
            """.trimIndent()
        )

        // A message as AKHQ shows it: the plain value of a union, the timestamp as ISO text.
        private val akhqJson = """{"id":1,"customerName":"Ada","createdAt":"2024-05-01T10:15:30.123Z"}"""

        // The same message in the Avro JSON encoding, which the consumer shows.
        private val avroJson = """{"id":1,"customerName":{"string":"Ada"},"createdAt":1714558530123}"""

        private fun avroConfig(type: KafkaFieldType, valueText: String) = ConsumerProducerFieldConfig(
            type = type,
            valueText = valueText,
            isKey = false,
            topic = "orders",
            registryType = KafkaRegistryType.CONFLUENT,
            schemaName = "orders-value",
            schemaFormat = KafkaRegistryFormat.AVRO,
            parsedSchema = avroSchema
        )

        private fun assertOrder(value: Any?) {
            val record = value as GenericRecord
            assertEquals(1L, record.get("id"))
            assertEquals("Ada", record.get("customerName").toString())
            assertEquals(1714558530123L, record.get("createdAt"))
        }

        @Test
        fun `should read the Avro JSON encoding for SCHEMA_REGISTRY type`() {
            assertOrder(avroConfig(KafkaFieldType.SCHEMA_REGISTRY, avroJson).getValueObj())
        }

        @Test
        fun `should read plain JSON as shown by AKHQ for SCHEMA_REGISTRY type`() {
            assertOrder(avroConfig(KafkaFieldType.SCHEMA_REGISTRY, akhqJson).getValueObj())
        }

        @Test
        fun `should read the Avro JSON encoding for AVRO_CUSTOM type`() {
            assertOrder(avroConfig(KafkaFieldType.AVRO_CUSTOM, avroJson).getValueObj())
        }

        @Test
        fun `should read plain JSON as shown by AKHQ for AVRO_CUSTOM type`() {
            assertOrder(avroConfig(KafkaFieldType.AVRO_CUSTOM, akhqJson).getValueObj())
        }

        @Test
        fun `should name the field that does not fit`() {
            val failure = assertThrows<Exception> {
                avroConfig(KafkaFieldType.SCHEMA_REGISTRY, akhqJson.replace("\"id\":1", "\"id\":\"one\"")).getValueObj()
            }

            assertTrue(failure.message!!.startsWith("\$.id: expected an integer"), failure.message)
        }
    }
}
