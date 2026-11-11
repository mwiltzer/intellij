package io.confluent.intellijplugin.producer.client

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.notMatching
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.BdtKafkaAvroSerializer
import io.confluent.intellijplugin.common.models.BdtKafkaJsonSchemaSerializer
import io.confluent.intellijplugin.common.models.BdtKafkaProtobufSerializer
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.consumer.models.ConsumerProducerFieldConfig
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.confluent.ConfluentRegistryClient
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.context.NullContextNameStrategy
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

/**
 * The producer hands the schema of the chosen version to the serializer, which looks that schema up to get its ID.
 * These tests run the serializers of the producer against a registry that gives every version its own ID.
 */
@TestApplication
class VersionedSchemaSerializationTest {

    companion object {
        private lateinit var registry: WireMockServer

        @JvmStatic
        @BeforeAll
        fun startRegistry() {
            registry = WireMockServer(wireMockConfig().dynamicPort())
            registry.start()
        }

        @JvmStatic
        @AfterAll
        fun stopRegistry() {
            registry.stop()
        }
    }

    @AfterEach
    fun reset() {
        registry.resetAll()
    }

    private val url get() = "http://localhost:${registry.port()}"

    private val avroV1 = """{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"}]}"""
    private val avroV2 = """{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}"""
    private val protoV1 = "syntax = \"proto3\";\npackage com.example;\n\nmessage Order {\n  string id = 1;\n}\n"
    private val protoV2 = "syntax = \"proto3\";\npackage com.example;\n\nmessage Order {\n  string id = 1;\n  string note = 2;\n}\n"
    private val jsonV1 = """{"type":"object","properties":{"id":{"type":"string"}}}"""
    private val jsonV2 = """{"type":"object","properties":{"id":{"type":"string"},"note":{"type":"string"}}}"""

    /** The registry answers a lookup of a schema with the ID of the version that has it: 1 without a note, 2 with. */
    private fun stubLookups() {
        fun answer(version: Int, id: Int) =
            aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                .withBody("""{"subject":"orders-value","version":$version,"id":$id,"schema":"{}"}""")

        registry.stubFor(
            post(urlPathEqualTo("/subjects/orders-value")).withRequestBody(containing("note")).willReturn(answer(2, 22))
        )
        registry.stubFor(
            post(urlPathEqualTo("/subjects/orders-value")).withRequestBody(notMatching("(?s).*note.*")).willReturn(answer(1, 11))
        )
    }

    private fun config(format: KafkaRegistryFormat, schema: ParsedSchema, valueText: String, version: Long?) =
        ConsumerProducerFieldConfig(
            type = KafkaFieldType.SCHEMA_REGISTRY,
            valueText = valueText,
            isKey = false,
            topic = "orders",
            registryType = KafkaRegistryType.CONFLUENT,
            schemaName = "orders-value",
            schemaFormat = format,
            parsedSchema = schema,
            schemaVersion = version
        )

    private fun <T : Serializer<*>> configured(serializer: T): T = serializer.also {
        it.configure(
            mapOf(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to url,
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to false,
                AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY to NullContextNameStrategy::class.java
            ),
            false
        )
    }

    private fun schemaIdOf(bytes: ByteArray?): Int {
        requireNotNull(bytes)
        assertEquals(0.toByte(), bytes[0], "magic byte")
        return ByteBuffer.wrap(bytes, 1, 4).getInt()
    }

    @Suppress("UNCHECKED_CAST")
    private fun serialize(serializer: Serializer<*>, field: ConsumerProducerFieldConfig) =
        (serializer as Serializer<Any?>).serialize("orders", field.getValueObj())

    private fun lookupsWith(text: String) = postRequestedFor(urlPathEqualTo("/subjects/orders-value")).withRequestBody(containing(text))

    private fun client() = ConfluentRegistryClient.create(url, emptyMap()).internalClient

    @Test
    fun `should write Avro with the ID of the chosen version`() {
        stubLookups()
        val v1 = AvroSchema(avroV1)
        val v2 = AvroSchema(avroV2)

        val withV1 = serialize(configured(BdtKafkaAvroSerializer(client(), "orders-value", v1)), config(KafkaRegistryFormat.AVRO, v1, """{"id":"a"}""", 1))
        val withV2 = serialize(configured(BdtKafkaAvroSerializer(client(), "orders-value", v2)), config(KafkaRegistryFormat.AVRO, v2, """{"id":"a","note":{"string":"hi"}}""", 2))

        assertEquals(11, schemaIdOf(withV1))
        assertEquals(22, schemaIdOf(withV2))
    }

    @Test
    fun `should ask for the schema of the chosen version, not for the latest one`() {
        stubLookups()
        val v1 = AvroSchema(avroV1)

        serialize(configured(BdtKafkaAvroSerializer(client(), "orders-value", v1)), config(KafkaRegistryFormat.AVRO, v1, """{"id":"a"}""", 1))

        registry.verify(1, postRequestedFor(urlPathEqualTo("/subjects/orders-value")))
        registry.verify(0, lookupsWith("note"))
    }

    @Test
    fun `should write Protobuf with the ID of the chosen version`() {
        stubLookups()
        val v1 = ProtobufSchema(protoV1)
        val v2 = ProtobufSchema(protoV2)

        val withV1 = serialize(configured(BdtKafkaProtobufSerializer(client(), "orders-value", v1)), config(KafkaRegistryFormat.PROTOBUF, v1, """{"id":"a"}""", 1))
        val withV2 = serialize(configured(BdtKafkaProtobufSerializer(client(), "orders-value", v2)), config(KafkaRegistryFormat.PROTOBUF, v2, """{"id":"a","note":"hi"}""", 2))

        assertEquals(11, schemaIdOf(withV1))
        assertEquals(22, schemaIdOf(withV2))
    }

    @Test
    fun `should write JSON Schema with the ID of the chosen version`() {
        stubLookups()
        val v1 = JsonSchema(jsonV1)
        val v2 = JsonSchema(jsonV2)

        val withV1 = serialize(configured(BdtKafkaJsonSchemaSerializer(client(), "orders-value")), config(KafkaRegistryFormat.JSON, v1, """{"id":"a"}""", 1))
        val withV2 = serialize(configured(BdtKafkaJsonSchemaSerializer(client(), "orders-value")), config(KafkaRegistryFormat.JSON, v2, """{"id":"a","note":"hi"}""", 2))

        assertEquals(11, schemaIdOf(withV1))
        assertEquals(22, schemaIdOf(withV2))
    }
}
