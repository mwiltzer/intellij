package io.confluent.intellijplugin.common.models

import io.confluent.intellijplugin.client.KafkaClient
import io.confluent.intellijplugin.consumer.models.ConsumerProducerFieldConfig
import io.confluent.intellijplugin.data.KafkaDataManager
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.RecordSchema
import io.confluent.intellijplugin.registry.SchemaIdHeaders
import io.confluent.intellijplugin.registry.confluent.ConfluentRegistryClient
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.nio.ByteBuffer

class SchemaIdRecordingDeserializerTest {

    private val avro = Schema.Parser().parse(
        """{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"""
    )

    private val props = mapOf<String, Any>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://test")

    private fun user(name: String) = GenericData.Record(avro).apply { put("name", name) }

    /** A registry in which a few other schemas were registered first, so the ID of the one of interest is not 1. */
    private fun registry(): MockSchemaRegistryClient = MockSchemaRegistryClient().apply {
        repeat(3) { i ->
            register("other-$i-value", AvroSchema(Schema.Parser().parse(
                """{"type":"record","name":"Other$i","fields":[{"name":"x","type":"int"}]}"""
            )))
        }
    }

    private fun serialized(client: MockSchemaRegistryClient, topic: String, isKey: Boolean = false, name: String = "Ada"):
            Pair<ByteArray, Int> {
        val serializer = KafkaAvroSerializer(client, props)
        serializer.configure(props, isKey)
        val bytes = serializer.serialize(topic, user(name))
        // The ID the serializer wrote after the magic byte, which the registry gave the schema.
        return bytes to ByteBuffer.wrap(bytes, 1, 4).getInt()
    }

    @Nested
    inner class WithTheSerializersOfTheRegistry {

        @Test
        fun `should note the schema ID of a value and still deserialize it`() {
            val client = registry()
            val (bytes, id) = serialized(client, "orders")
            val deserializer = SchemaIdRecordingDeserializer<Any?>(KafkaAvroDeserializer(client)).apply {
                configure(props, false)
            }
            val headers = RecordHeaders()

            val value = deserializer.deserialize("orders", headers, bytes)

            assertEquals("Ada", (value as GenericRecord).get("name").toString())
            assertEquals(RecordSchema(id = id), SchemaIdHeaders.schemaOf(headers, isKey = false))
            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = true))
        }

        @Test
        fun `should note the schema ID of a key when it is configured for keys`() {
            val client = registry()
            val (bytes, id) = serialized(client, "orders", isKey = true)
            val deserializer = SchemaIdRecordingDeserializer<Any?>(KafkaAvroDeserializer(client)).apply {
                configure(props, true)
            }
            val headers = RecordHeaders()

            deserializer.deserialize("orders", headers, bytes)

            assertEquals(RecordSchema(id = id), SchemaIdHeaders.schemaOf(headers, isKey = true))
            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should work the way the Kafka client calls it, with the payload as a buffer`() {
            val client = registry()
            val (bytes, id) = serialized(client, "orders")
            val deserializer = SchemaIdRecordingDeserializer<Any?>(KafkaAvroDeserializer(client)).apply {
                configure(props, false)
            }
            val headers = RecordHeaders()

            val value = deserializer.deserialize("orders", headers, ByteBuffer.wrap(bytes))

            assertEquals("Ada", (value as GenericRecord).get("name").toString())
            assertEquals(RecordSchema(id = id), SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should tell the schemas of two records apart`() {
            val client = registry()
            val (first, firstId) = serialized(client, "orders", name = "Ada")
            val newer = Schema.Parser().parse(
                """{"type":"record","name":"User","fields":[{"name":"name","type":"string"},
                   {"name":"age","type":["null","int"],"default":null}]}"""
            )
            val newerId = client.register("orders-value", AvroSchema(newer))
            val serializer = KafkaAvroSerializer(client, props)
            val second = serializer.serialize("orders", GenericData.Record(newer).apply { put("name", "Bo") })
            val deserializer = SchemaIdRecordingDeserializer<Any?>(KafkaAvroDeserializer(client)).apply {
                configure(props, false)
            }
            val headersOfFirst = RecordHeaders()
            val headersOfSecond = RecordHeaders()

            deserializer.deserialize("orders", headersOfFirst, first)
            deserializer.deserialize("orders", headersOfSecond, second)

            assertEquals(firstId, SchemaIdHeaders.schemaOf(headersOfFirst, isKey = false)?.id)
            assertEquals(newerId, SchemaIdHeaders.schemaOf(headersOfSecond, isKey = false)?.id)
            assertTrue(newerId != firstId)
        }

        @Test
        fun `should let the failure of the deserializer through`() {
            val client = registry()
            val unknownId = byteArrayOf(0, 0, 0, 9, 9, 9, 1, 2)
            val deserializer = SchemaIdRecordingDeserializer<Any?>(KafkaAvroDeserializer(client)).apply {
                configure(props, false)
            }

            assertThrows(SerializationException::class.java) {
                deserializer.deserialize("orders", RecordHeaders(), unknownId)
            }
        }

        @Test
        fun `should note nothing for a payload that does not use the registry`() {
            val deserializer = SchemaIdRecordingDeserializer<String>(StringDeserializer())
            val headers = RecordHeaders()

            val value = deserializer.deserialize("orders", headers, "plain".toByteArray())

            assertEquals("plain", value)
            assertEquals(0, headers.toArray().size)
        }
    }

    @Nested
    inner class Delegating {

        @Suppress("UNCHECKED_CAST")
        private val delegate: Deserializer<String> = mock()

        @Test
        fun `should configure the deserializer it wraps, for keys and for values`() {
            val deserializer = SchemaIdRecordingDeserializer(delegate)

            deserializer.configure(props, true)
            deserializer.configure(props, false)

            verify(delegate).configure(props, true)
            verify(delegate).configure(props, false)
        }

        @Test
        fun `should close the deserializer it wraps`() {
            SchemaIdRecordingDeserializer(delegate).close()

            verify(delegate).close()
        }

        @Test
        fun `should pass a payload without headers on, noting nothing`() {
            whenever(delegate.deserialize("t", byteArrayOf(0, 0, 0, 0, 5))).thenReturn("x")

            val value = SchemaIdRecordingDeserializer(delegate).deserialize("t", byteArrayOf(0, 0, 0, 0, 5))

            assertEquals("x", value)
        }

        @Test
        fun `should hand the same headers on to the deserializer it wraps`() {
            val headers = RecordHeaders()
            val seen = ArrayList<Headers?>()
            val recording = SchemaIdRecordingDeserializer(object : Deserializer<String> {
                override fun deserialize(topic: String?, data: ByteArray?): String = "no headers"
                override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): String {
                    seen += headers
                    return "with headers"
                }
            })

            val value = recording.deserialize("t", headers, byteArrayOf(0, 0, 0, 0, 5))

            assertEquals("with headers", value)
            assertSame(headers, seen.single())
            // The deserializer it wraps could see what was noted, but none of it may be mistaken for a producer's header.
            assertEquals(5, SchemaIdHeaders.schemaOf(headers, isKey = false)?.id)
        }

        @Test
        fun `should be asked for keys unless it is configured for values`() {
            val headers = RecordHeaders()
            val recording = SchemaIdRecordingDeserializer(delegate, isKey = true)
            whenever(delegate.deserialize(any<String>(), any<Headers>(), any<ByteArray>())).thenReturn("x")

            recording.deserialize("t", headers, byteArrayOf(0, 0, 0, 0, 5))

            assertEquals(5, SchemaIdHeaders.schemaOf(headers, isKey = true)?.id)
            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }
    }

    @Nested
    inner class AsTheConsumerGetsIt {

        private fun dataManager(type: KafkaRegistryType): KafkaDataManager {
            val registryClient = mock<ConfluentRegistryClient> {
                on { internalClient } doReturn mock<CachedSchemaRegistryClient>()
            }
            val kafkaClient = mock<KafkaClient> { on { confluentRegistryClient } doReturn registryClient }
            return mock { on { client } doReturn kafkaClient; on { registryType } doReturn type }
        }

        private fun field(format: KafkaRegistryFormat) = ConsumerProducerFieldConfig(
            type = KafkaFieldType.SCHEMA_REGISTRY,
            valueText = "",
            isKey = false,
            topic = "orders",
            registryType = KafkaRegistryType.CONFLUENT,
            schemaName = "orders-value",
            schemaFormat = format,
            parsedSchema = null
        )

        @Test
        fun `should record the schema for the formats of the registry`() {
            for (format in listOf(KafkaRegistryFormat.AVRO, KafkaRegistryFormat.PROTOBUF, KafkaRegistryFormat.JSON)) {
                val deserializer = KafkaFieldType.SCHEMA_REGISTRY
                    .getDeserializationClass(dataManager(KafkaRegistryType.CONFLUENT), field(format))

                assertInstanceOf(SchemaIdRecordingDeserializer::class.java, deserializer, "format $format")
            }
        }

        @Test
        fun `should not wrap the deserializers of the other types`() {
            val dataManager = dataManager(KafkaRegistryType.CONFLUENT)

            for (type in listOf(KafkaFieldType.STRING, KafkaFieldType.JSON, KafkaFieldType.LONG, KafkaFieldType.BASE64)) {
                val deserializer = type.getDeserializationClass(dataManager, field(KafkaRegistryFormat.AVRO))

                assertTrue(deserializer !is SchemaIdRecordingDeserializer<*>, "type $type")
            }
        }
    }
}
