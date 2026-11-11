package io.confluent.intellijplugin.registry

import io.confluent.kafka.serializers.schema.id.SchemaId
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import java.util.UUID

class RecordSchemaTest {

    /** What the registry's serializers write: the magic byte, the ID, then the payload. */
    private fun v0(id: Int, payload: ByteArray = byteArrayOf(1, 2, 3)): ByteArray =
        ByteBuffer.allocate(5 + payload.size).put(SchemaId.MAGIC_BYTE_V0).putInt(id).put(payload).array()

    private fun v1Header(guid: UUID, extra: Int = 0): ByteArray =
        ByteBuffer.allocate(17 + extra).put(SchemaId.MAGIC_BYTE_V1)
            .putLong(guid.mostSignificantBits).putLong(guid.leastSignificantBits).array()

    @Nested
    inner class Recording {

        @Test
        fun `should note the ID of a payload in the wire format V0 for a value`() {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(100016))

            assertEquals(RecordSchema(id = 100016), SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should keep the ID of the key and of the value apart`() {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = true, payload = v0(7))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(9))

            assertEquals(RecordSchema(id = 7), SchemaIdHeaders.schemaOf(headers, isKey = true))
            assertEquals(RecordSchema(id = 9), SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should tell nothing about the value when only the key was noted`() {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = true, payload = v0(7))

            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @ParameterizedTest
        @ValueSource(ints = [0, 1, 255, 256, 65_535, 16_777_216, Int.MAX_VALUE])
        fun `should read the ID big-endian`(id: Int) {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(id))

            assertEquals(id, SchemaIdHeaders.schemaOf(headers, isKey = false)?.id)
        }

        @Test
        fun `should note the ID of a payload that has nothing but the prefix`() {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(5, payload = byteArrayOf()))

            assertEquals(5, SchemaIdHeaders.schemaOf(headers, isKey = false)?.id)
        }

        @Test
        fun `should note a later payload in place of an earlier one`() {
            val headers = RecordHeaders()

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(1))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(2))

            assertEquals(2, SchemaIdHeaders.schemaOf(headers, isKey = false)?.id)
            assertEquals(1, headers.count { SchemaIdHeaders.isReserved(it.key()) })
        }

        @Test
        fun `should not note a payload that is not in the wire format`() {
            val notWireFormats = listOf(
                "{\"a\":1}".toByteArray(),
                byteArrayOf(1, 0, 0, 0, 5, 9),
                byteArrayOf(0, 0, 0, 5),
                byteArrayOf(0),
                byteArrayOf()
            )
            for (payload in notWireFormats) {
                val headers = RecordHeaders()

                SchemaIdHeaders.record(headers, isKey = false, payload = payload)

                assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false), "payload ${payload.toList()}")
                assertEquals(0, headers.toArray().size)
            }
        }

        @Test
        fun `should do nothing without headers or payload`() {
            SchemaIdHeaders.record(null, isKey = false, payload = v0(1))

            val headers = RecordHeaders()
            SchemaIdHeaders.record(headers, isKey = false, payload = null)

            assertEquals(0, headers.toArray().size)
        }

        @Test
        fun `should leave headers alone that can not be changed`() {
            val headers = RecordHeaders(listOf(RecordHeader("a", "b".toByteArray()))).apply { setReadOnly() }

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(1))

            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
            assertEquals(1, headers.toArray().size)
        }

        @Test
        fun `should leave the headers of the producer as they are`() {
            val headers = RecordHeaders(listOf(RecordHeader("trace", "t1".toByteArray())))

            SchemaIdHeaders.record(headers, isKey = false, payload = v0(1))

            assertArrayEquals("t1".toByteArray(), headers.lastHeader("trace").value())
        }
    }

    @Nested
    inner class ReadingTheGuid {

        @Test
        fun `should read the GUID of the wire format V1 from the header of the value`() {
            val guid = UUID.randomUUID()
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, v1Header(guid))))

            assertEquals(RecordSchema(guid = guid), SchemaIdHeaders.schemaOf(headers, isKey = false))
            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = true))
        }

        @Test
        fun `should read the GUID of the key from the header of the key`() {
            val guid = UUID.randomUUID()
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.KEY_SCHEMA_ID_HEADER, v1Header(guid))))

            assertEquals(RecordSchema(guid = guid), SchemaIdHeaders.schemaOf(headers, isKey = true))
            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should read a GUID that is followed by more bytes`() {
            val guid = UUID.randomUUID()
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, v1Header(guid, extra = 4))))

            assertEquals(guid, SchemaIdHeaders.schemaOf(headers, isKey = false)?.guid)
        }

        @Test
        fun `should not read a header that is too short or has another magic byte`() {
            val guid = UUID.randomUUID()
            val tooShort = v1Header(guid).copyOf(16)
            val otherMagic = v1Header(guid).also { it[0] = 7 }

            for (value in listOf(tooShort, otherMagic, byteArrayOf())) {
                val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, value)))
                assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false), "header ${value.toList()}")
            }
        }

        @Test
        fun `should tell nothing when the header has no value`() {
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, null)))

            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should prefer the ID that was noted to the GUID`() {
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, v1Header(UUID.randomUUID()))))
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(12))

            assertEquals(RecordSchema(id = 12), SchemaIdHeaders.schemaOf(headers, isKey = false))
        }

        @Test
        fun `should tell nothing about headers that are missing or have no schema`() {
            assertNull(SchemaIdHeaders.schemaOf(null, isKey = false))
            assertNull(SchemaIdHeaders.schemaOf(RecordHeaders(), isKey = false))
            assertNull(SchemaIdHeaders.schemaOf(RecordHeaders(listOf(RecordHeader("a", byteArrayOf(0, 0, 0, 1)))), false))
        }

        @Test
        fun `should not take an ID header of another size for an ID`() {
            val headers = RecordHeaders()
            headers.add("confluent.intellij.value.schemaId", byteArrayOf(0, 0, 1))

            assertNull(SchemaIdHeaders.schemaOf(headers, isKey = false))
        }
    }

    @Nested
    inner class VisibleHeaders {

        @Test
        fun `should leave out what was noted and keep what the producer sent, in order`() {
            val headers = RecordHeaders()
            headers.add("a", "1".toByteArray())
            SchemaIdHeaders.record(headers, isKey = true, payload = v0(1))
            headers.add("b", "2".toByteArray())
            SchemaIdHeaders.record(headers, isKey = false, payload = v0(2))
            headers.add("a", "3".toByteArray())

            val visible = SchemaIdHeaders.visible(headers)

            assertEquals(listOf("a", "b", "a"), visible.map { it.key() })
            assertEquals(listOf("1", "2", "3"), visible.map { String(it.value()) })
        }

        @Test
        fun `should keep the header of the wire format V1, which the producer sent`() {
            val headers = RecordHeaders(listOf(RecordHeader(SchemaId.VALUE_SCHEMA_ID_HEADER, v1Header(UUID.randomUUID()))))

            assertEquals(listOf(SchemaId.VALUE_SCHEMA_ID_HEADER), SchemaIdHeaders.visible(headers).map { it.key() })
        }

        @Test
        fun `should have nothing visible without headers`() {
            assertTrue(SchemaIdHeaders.visible(null).isEmpty())
            assertTrue(SchemaIdHeaders.visible(RecordHeaders()).isEmpty())
        }

        @Test
        fun `should tell the reserved names from the others`() {
            assertTrue(SchemaIdHeaders.isReserved("confluent.intellij.key.schemaId"))
            assertTrue(SchemaIdHeaders.isReserved("confluent.intellij.value.schemaId"))
            assertFalse(SchemaIdHeaders.isReserved(SchemaId.VALUE_SCHEMA_ID_HEADER))
            assertFalse(SchemaIdHeaders.isReserved(SchemaId.KEY_SCHEMA_ID_HEADER))
            assertFalse(SchemaIdHeaders.isReserved("trace"))
            assertFalse(SchemaIdHeaders.isReserved(""))
            assertFalse(SchemaIdHeaders.isReserved(null))
        }
    }
}
