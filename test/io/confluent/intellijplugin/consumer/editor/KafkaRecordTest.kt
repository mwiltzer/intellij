package io.confluent.intellijplugin.consumer.editor

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.RecordSchema
import io.confluent.intellijplugin.registry.SchemaIdHeaders
import io.confluent.kafka.serializers.schema.id.SchemaId
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.*

@TestApplication
class KafkaRecordTest {

    private fun consumerRecord(
        key: Any? = "test-key",
        value: Any? = "test-value",
        topic: String = "test-topic",
        partition: Int = 0,
        offset: Long = 42L,
        timestamp: Long = 1700000000000L,
        keySize: Int = 8,
        valueSize: Int = 10,
        headers: RecordHeaders = RecordHeaders()
    ): ConsumerRecord<Any, Any> = ConsumerRecord(
        topic, partition, offset, timestamp,
        TimestampType.CREATE_TIME, keySize, valueSize,
        key, value, headers, Optional.empty()
    )

    @Nested
    @DisplayName("createFor - successful ConsumerRecord")
    inner class CreateForSuccess {

        @Test
        fun `should map topic, partition, offset, and timestamp from ConsumerRecord`() {
            val rec = consumerRecord(
                topic = "my-topic", partition = 3, offset = 100L, timestamp = 1700000000000L
            )
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertEquals("my-topic", kafkaRecord.topic)
            assertEquals(3, kafkaRecord.partition)
            assertEquals(100L, kafkaRecord.offset)
            assertEquals(1700000000000L, kafkaRecord.timestamp)
        }

        @Test
        fun `should map key and value from ConsumerRecord`() {
            val rec = consumerRecord(key = "k1", value = "v1")
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertEquals("k1", kafkaRecord.keyText)
            assertEquals("v1", kafkaRecord.valueText)
        }

        @Test
        fun `should extract headers as Property list`() {
            val headers = RecordHeaders(listOf(
                RecordHeader("h1", "val1".toByteArray()),
                RecordHeader("h2", "val2".toByteArray())
            ))
            val rec = consumerRecord(headers = headers)
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertEquals(2, kafkaRecord.headers.size)
            assertEquals("h1", kafkaRecord.headers[0].name)
            assertEquals("val1", kafkaRecord.headers[0].value)
            assertEquals("h2", kafkaRecord.headers[1].name)
            assertEquals("val2", kafkaRecord.headers[1].value)
        }

        @Test
        fun `should set error to null for successful records`() {
            val rec = consumerRecord()
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertNull(kafkaRecord.error)
        }

        @Test
        fun `should default keyType to STRING when passed null`() {
            val rec = consumerRecord()
            val kafkaRecord = KafkaRecord.createFor(
                null, null,
                null, null,
                Result.success(rec)
            )
            assertEquals(KafkaFieldType.STRING, kafkaRecord.keyType)
            assertEquals(KafkaFieldType.STRING, kafkaRecord.valueType)
        }

        @Test
        fun `should default keyFormat and valueFormat to UNKNOWN when passed null`() {
            val rec = consumerRecord()
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                null, null,
                Result.success(rec)
            )
            assertEquals(KafkaRegistryFormat.UNKNOWN, kafkaRecord.keyFormat)
            assertEquals(KafkaRegistryFormat.UNKNOWN, kafkaRecord.valueFormat)
        }

        @Test
        fun `should map serialized key and value sizes`() {
            val rec = consumerRecord(keySize = 15, valueSize = 200)
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertEquals(15, kafkaRecord.keySize)
            assertEquals(200, kafkaRecord.valueSize)
        }

        @Test
        fun `should handle empty headers`() {
            val rec = consumerRecord(headers = RecordHeaders())
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertTrue(kafkaRecord.headers.isEmpty())
        }
    }

    @Nested
    @DisplayName("createFor - failed Result")
    inner class CreateForFailure {

        @Test
        fun `should create error record from failed Result with exception details`() {
            val exception = RuntimeException("Deserialization failed")
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(exception)
            )
            assertEquals(exception, kafkaRecord.error)
        }

        @Test
        fun `should set key and value text to null for failed records`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail"))
            )
            assertNull(kafkaRecord.keyText)
            assertNull(kafkaRecord.valueText)
        }

        @Test
        fun `should use errorPartition and errorOffset for failed records`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail")),
                errorPartition = 5,
                errorOffset = 99L
            )
            assertEquals(5, kafkaRecord.partition)
            assertEquals(99L, kafkaRecord.offset)
        }

        @Test
        fun `should default partition to -1 and offset to -1 when not provided`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail"))
            )
            assertEquals(-1, kafkaRecord.partition)
            assertEquals(-1L, kafkaRecord.offset)
        }

        @Test
        fun `should set empty topic and empty headers for failed records`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail"))
            )
            assertEquals("", kafkaRecord.topic)
            assertTrue(kafkaRecord.headers.isEmpty())
        }

        @Test
        fun `should set keyFormat and valueFormat independently for failed records`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.AVRO, KafkaRegistryFormat.PROTOBUF,
                Result.failure(RuntimeException("fail"))
            )
            assertEquals(KafkaRegistryFormat.AVRO, kafkaRecord.keyFormat)
            assertEquals(KafkaRegistryFormat.PROTOBUF, kafkaRecord.valueFormat)
        }
    }

    @Nested
    @DisplayName("Computed properties")
    inner class ComputedProperties {

        @Test
        fun `should compute keyText using KafkaEditorUtils when no error`() {
            val rec = consumerRecord(key = "my-key")
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            // For STRING type with UNKNOWN format, getValueAsString returns toString()
            assertEquals("my-key", kafkaRecord.keyText)
        }

        @Test
        fun `should compute valueText using KafkaEditorUtils when no error`() {
            val rec = consumerRecord(value = "my-value")
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            assertEquals("my-value", kafkaRecord.valueText)
        }

        @Test
        fun `should return null for keyText when error is present`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail"))
            )
            assertNull(kafkaRecord.keyText)
        }

        @Test
        fun `should return null for valueText when error is present`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("fail"))
            )
            assertNull(kafkaRecord.valueText)
        }

        @Test
        fun `should compute errorText from exception message`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException("Something went wrong"))
            )
            assertEquals("Something went wrong", kafkaRecord.errorText)
        }

        @Test
        fun `should compute errorText as class simple name when message is null`() {
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.failure(RuntimeException())
            )
            assertEquals("RuntimeException", kafkaRecord.errorText)
        }

        @Test
        fun `should return empty string for keyText when key is null and no error`() {
            val rec = consumerRecord(key = null)
            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.STRING,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN,
                Result.success(rec)
            )
            // KafkaEditorUtils.getValueAsString returns "" when value is null
            assertEquals("", kafkaRecord.keyText)
        }
    }

    @Nested
    @DisplayName("Retained memory")
    inner class RetainedMemory {

        @Test
        fun `should render a deserialized Avro record to text when the record is created`() {
            val schema = Schema.Parser().parse(
                """{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"""
            )
            val avro = GenericData.Record(schema).apply {
                put("name", "Ada")
                put("age", 36)
            }

            val kafkaRecord = KafkaRecord.createFor(
                KafkaFieldType.STRING, KafkaFieldType.SCHEMA_REGISTRY,
                KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.AVRO,
                Result.success(consumerRecord(value = avro))
            )

            val text = kafkaRecord.valueText!!
            assertTrue(text.contains("\"name\""), text)
            assertTrue(text.contains("Ada"), text)
            assertTrue(text.contains("36"), text)
        }

        @Test
        fun `should declare no field that could keep the deserialized key or value alive`() {
            val untypedFields = KafkaRecord::class.java.declaredFields
                .filter { it.type == Any::class.java }
                .map { it.name }

            assertEquals(emptyList<String>(), untypedFields)
        }
    }

    @Nested
    @DisplayName("Schema of the record")
    inner class SchemaOfTheRecord {

        private val avro = Schema.Parser().parse(
            """{"type":"record","name":"Person","fields":[{"name":"name","type":"string"}]}"""
        )

        private fun person() = GenericData.Record(avro).apply { put("name", "Ada") }

        private fun v0(id: Int) = byteArrayOf(0, (id shr 24).toByte(), (id shr 16).toByte(), (id shr 8).toByte(), id.toByte(), 9)

        private fun createFor(
            keyType: KafkaFieldType,
            valueType: KafkaFieldType,
            headers: RecordHeaders
        ) = KafkaRecord.createFor(
            keyType, valueType, KafkaRegistryFormat.AVRO, KafkaRegistryFormat.AVRO,
            Result.success(consumerRecord(key = person(), value = person(), headers = headers))
        )

        @Test
        fun `should tell the schema IDs of a key and a value that use the registry`() {
            val headers = RecordHeaders()
            SchemaIdHeaders.record(headers, isKey = true, payload = v0(11))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(22))

            val record = createFor(KafkaFieldType.SCHEMA_REGISTRY, KafkaFieldType.SCHEMA_REGISTRY, headers)

            assertEquals(RecordSchema(id = 11), record.keySchema)
            assertEquals(RecordSchema(id = 22), record.valueSchema)
        }

        @Test
        fun `should tell the schema of the value alone when the key does not use the registry`() {
            val headers = RecordHeaders()
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(22))

            val record = createFor(KafkaFieldType.STRING, KafkaFieldType.SCHEMA_REGISTRY, headers)

            assertNull(record.keySchema)
            assertEquals(RecordSchema(id = 22), record.valueSchema)
        }

        @Test
        fun `should tell the GUID of a schema that the producer wrote into the headers`() {
            val guid = UUID.randomUUID()
            val header = java.nio.ByteBuffer.allocate(17).put(1)
                .putLong(guid.mostSignificantBits).putLong(guid.leastSignificantBits).array()
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, header)))

            val record = createFor(KafkaFieldType.STRING, KafkaFieldType.SCHEMA_REGISTRY, headers)

            assertEquals(RecordSchema(guid = guid), record.valueSchema)
        }

        @Test
        fun `should tell nothing about a schema when the record says nothing`() {
            val record = createFor(KafkaFieldType.SCHEMA_REGISTRY, KafkaFieldType.SCHEMA_REGISTRY, RecordHeaders())

            assertNull(record.keySchema)
            assertNull(record.valueSchema)
        }

        @Test
        fun `should not tell a schema for a key or value that is not read with the registry`() {
            val headers = RecordHeaders()
            SchemaIdHeaders.record(headers, isKey = true, payload = v0(11))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(22))
            headers.add(SchemaId.VALUE_SCHEMA_ID_HEADER, ByteArray(17).also { it[0] = 1 })

            val record = createFor(KafkaFieldType.STRING, KafkaFieldType.BASE64, headers)

            assertNull(record.keySchema)
            assertNull(record.valueSchema)
        }

        @Test
        fun `should leave what was noted for the schema out of the headers`() {
            val headers = RecordHeaders()
            headers.add("trace", "t1".toByteArray())
            SchemaIdHeaders.record(headers, isKey = true, payload = v0(11))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(22))

            val record = createFor(KafkaFieldType.SCHEMA_REGISTRY, KafkaFieldType.SCHEMA_REGISTRY, headers)

            assertEquals(listOf("trace"), record.headers.map { it.name })
        }

        @Test
        fun `should keep a header of the producer that carries the schema`() {
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, ByteArray(17).also { it[0] = 1 })))

            val record = createFor(KafkaFieldType.STRING, KafkaFieldType.STRING, headers)

            assertEquals(listOf(SchemaId.VALUE_SCHEMA_ID_HEADER), record.headers.map { it.name })
        }

        @Test
        fun `should tell no schema for a record that failed`() {
            val record = KafkaRecord.createFor(
                KafkaFieldType.SCHEMA_REGISTRY, KafkaFieldType.SCHEMA_REGISTRY,
                KafkaRegistryFormat.AVRO, KafkaRegistryFormat.AVRO,
                Result.failure(IllegalStateException("boom"))
            )

            assertNull(record.keySchema)
            assertNull(record.valueSchema)
        }

        @Test
        fun `should tell no schema for a record the producer wrote`() {
            val record = KafkaRecord(
                KafkaFieldType.STRING, KafkaFieldType.STRING, null, "k", "v", "t", 0, 1, 2, 3, 1, 1,
                emptyList(), KafkaRegistryFormat.UNKNOWN, KafkaRegistryFormat.UNKNOWN
            )

            assertNull(record.keySchema)
            assertNull(record.valueSchema)
        }
    }
}
