package io.confluent.intellijplugin.registry

import com.intellij.testFramework.junit5.TestApplication
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

@TestApplication
class KafkaRegistryFormatTest {

    @Nested
    @DisplayName("fromSchemaType")
    inner class FromSchemaType {

        @Test
        fun `should default to AVRO when schemaType is null`() {
            assertEquals(KafkaRegistryFormat.AVRO, KafkaRegistryFormat.fromSchemaType(null))
        }

        @Test
        fun `should map AVRO string to AVRO`() {
            assertEquals(KafkaRegistryFormat.AVRO, KafkaRegistryFormat.fromSchemaType("AVRO"))
        }

        @Test
        fun `should be case-insensitive`() {
            assertEquals(KafkaRegistryFormat.AVRO, KafkaRegistryFormat.fromSchemaType("avro"))
        }

        @Test
        fun `should map PROTOBUF string to PROTOBUF`() {
            assertEquals(KafkaRegistryFormat.PROTOBUF, KafkaRegistryFormat.fromSchemaType("PROTOBUF"))
        }

        @Test
        fun `should map JSON string to JSON`() {
            assertEquals(KafkaRegistryFormat.JSON, KafkaRegistryFormat.fromSchemaType("JSON"))
        }

        @Test
        fun `should return UNKNOWN for unrecognized types`() {
            assertEquals(KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.fromSchemaType("UNKNOWN_TYPE"))
        }
    }

    @Nested
    @DisplayName("fromLoadedSchemaType")
    inner class FromLoadedSchemaType {

        @Test
        fun `should not know the format of a schema whose details are not loaded`() {
            assertNull(KafkaRegistryFormat.fromLoadedSchemaType(version = null, type = null))
        }

        @Test
        fun `should read a missing type as AVRO once the details are loaded`() {
            assertEquals(KafkaRegistryFormat.AVRO, KafkaRegistryFormat.fromLoadedSchemaType(version = 3, type = null))
        }

        @Test
        fun `should map the type of a schema whose details are loaded`() {
            assertEquals(
                KafkaRegistryFormat.PROTOBUF, KafkaRegistryFormat.fromLoadedSchemaType(version = 1, type = "PROTOBUF")
            )
            assertEquals(KafkaRegistryFormat.JSON, KafkaRegistryFormat.fromLoadedSchemaType(version = 1, type = "json"))
        }

        @Test
        fun `should not trust a type that came without a version`() {
            assertNull(KafkaRegistryFormat.fromLoadedSchemaType(version = null, type = "PROTOBUF"))
        }
    }
}
