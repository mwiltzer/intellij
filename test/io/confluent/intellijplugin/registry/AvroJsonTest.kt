package io.confluent.intellijplugin.registry

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.FloatNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.LongNode
import io.confluent.intellijplugin.util.generator.AvroGenerator
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericFixed
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.common.errors.SerializationException
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.util.Base64

private const val ORDER_SCHEMA_TEXT = """
{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"long"},
  {"name":"customerName","type":["null","string"],"default":null},
  {"name":"createdAt","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"day","type":{"type":"int","logicalType":"date"}},
  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
  {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
  {"name":"address","type":["null",{"type":"record","name":"Address","fields":[
      {"name":"city","type":"string"},{"name":"zip","type":["null","string"],"default":null}]}],"default":null},
  {"name":"tags","type":{"type":"array","items":["null","string"]}},
  {"name":"payload","type":"bytes"}
]}
"""

// ORDER_AKHQ is the message [order] as AKHQ shows it.
private const val ORDER_AKHQ = """{"id":1,"customerName":"Ada","createdAt":"2024-05-01T10:15:30.123Z","day":"2024-05-01","price":12.34,"status":"PAID","address":{"city":"Munich","zip":"80331"},"tags":["a",null],"payload":"aGVsbG8="}"""

private val ORDER = AvroSchema(ORDER_SCHEMA_TEXT)
private val ORDER_RAW: Schema = ORDER.rawSchema()
private val ADDRESS_RAW: Schema = ORDER_RAW.getField("address").schema().types[1]
private val STATUS_RAW: Schema = ORDER_RAW.getField("status").schema()

// 2024-05-01 is day 19844, and 2024-05-01T10:15:30.123Z is 1714558530123 ms, 12.34 is 1234 with scale 2 (0x04D2).
private fun address(city: String = "Munich", zip: String? = "80331") = GenericData.Record(ADDRESS_RAW).apply {
    put("city", city)
    put("zip", zip)
}

private fun order(customerName: String? = "Ada", address: GenericData.Record? = address()) =
    GenericData.Record(ORDER_RAW).apply {
        put("id", 1L)
        put("customerName", customerName)
        put("createdAt", 1714558530123L)
        put("day", 19844)
        put("price", ByteBuffer.wrap(byteArrayOf(0x04, 0xD2.toByte())))
        put("status", GenericData.EnumSymbol(STATUS_RAW, "PAID"))
        put("address", address)
        put("tags", GenericData.Array(ORDER_RAW.getField("tags").schema(), listOf("a", null)))
        put("payload", ByteBuffer.wrap("hello".toByteArray()))
    }

private fun schemaOf(json: String): Schema = AvroSchema(json).rawSchema()

private fun encode(schema: Schema, datum: Any?): ByteArray {
    val out = ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    GenericDatumWriter<Any?>(schema).write(datum, encoder)
    encoder.flush()
    return out.toByteArray()
}

private fun avroJsonOf(schema: Schema, datum: Any?): String {
    val out = ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(schema, out)
    GenericDatumWriter<Any?>(schema).write(datum, encoder)
    encoder.flush()
    return out.toString(Charsets.UTF_8)
}

// What Kafka writes is the binary encoding, so equal bytes mean the same message, down to the type of every value.
private fun assertSameAvro(schema: Schema, expected: Any?, actual: Any?) =
    assertArrayEquals(encode(schema, expected), encode(schema, actual)) { "expected $expected but was $actual" }

private fun bytesOf(value: Any?): ByteArray = when (value) {
    is ByteBuffer -> ByteArray(value.remaining()).also { value.duplicate().get(it) }
    is GenericFixed -> value.bytes()
    else -> fail("not bytes: $value")
}

private fun primitive(type: String) = "\"$type\""

class AvroJsonTest {

    private fun plain(schemaText: String, json: String): Any? = AvroJson.readPlain(json, schemaOf(schemaText))

    private fun plainFailure(schemaText: String, json: String): String =
        assertThrows<SerializationException> { plain(schemaText, json) }.message!!

    @Nested
    @DisplayName("what AKHQ shows")
    inner class Akhq {

        @Test
        fun `a message with plain unions, ISO dates, a decimal number and base64 bytes is read`() {
            assertSameAvro(ORDER_RAW, order(), AvroJson.toObject(ORDER_AKHQ, ORDER))
        }

        @Test
        fun `null fields can be written as null`() {
            val json = ORDER_AKHQ.replace(""""customerName":"Ada"""", """"customerName":null""")
                .replace(""""address":{"city":"Munich","zip":"80331"}""", """"address":null""")

            assertSameAvro(ORDER_RAW, order(customerName = null, address = null), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `null fields can be left out`() {
            val json = ORDER_AKHQ.replace(""""customerName":"Ada",""", "")
                .replace(""""address":{"city":"Munich","zip":"80331"},""", "")

            assertSameAvro(ORDER_RAW, order(customerName = null, address = null), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `a null that is left out in a nested record is read as null`() {
            val json = ORDER_AKHQ.replace(""","zip":"80331"""", "")

            assertSameAvro(ORDER_RAW, order(address = address(zip = null)), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `dates and timestamps may also be raw numbers`() {
            val json = ORDER_AKHQ.replace(""""2024-05-01T10:15:30.123Z"""", "1714558530123")
                .replace(""""2024-05-01"""", "19844")

            assertSameAvro(ORDER_RAW, order(), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `a timestamp with an offset is the same instant`() {
            val json = ORDER_AKHQ.replace("2024-05-01T10:15:30.123Z", "2024-05-01T12:15:30.123+02:00")

            assertSameAvro(ORDER_RAW, order(), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `a union can be wrapped in one field and plain in the next`() {
            val json = ORDER_AKHQ.replace(""""customerName":"Ada"""", """"customerName":{"string":"Ada"}""")

            assertSameAvro(ORDER_RAW, order(), AvroJson.toObject(json, ORDER))
        }

        @Test
        fun `JSON is read as it is written over several lines`() {
            val json = """
                {
                  "id": 1,
                  "customerName": "Ada",
                  "createdAt": "2024-05-01T10:15:30.123Z",
                  "day": "2024-05-01",
                  "price": 12.34,
                  "status": "PAID",
                  "address": { "city": "Munich", "zip": "80331" },
                  "tags": [ "a", null ],
                  "payload": "aGVsbG8="
                }
            """.trimIndent()

            assertSameAvro(ORDER_RAW, order(), AvroJson.toObject(json, ORDER))
        }
    }

    @Nested
    @DisplayName("the Avro JSON encoding")
    inner class AvroEncoding {

        @Test
        fun `is read as before`() {
            val json = avroJsonOf(ORDER_RAW, order())

            val viaAvro = AvroSchemaUtils.toObject(json, ORDER)
            val viaPlugin = AvroJson.toObject(json, ORDER)

            assertTrue(viaPlugin is GenericData.Record)
            assertSameAvro(ORDER_RAW, viaAvro, viaPlugin)
            assertSameAvro(ORDER_RAW, order(), viaPlugin)
        }

        @Test
        fun `is read by Avro first, so input that was valid keeps its meaning`() {
            val schema = AvroSchema("""{"type":"record","name":"Blob","fields":[{"name":"payload","type":"bytes"}]}""")
            // Avro reads these characters as the bytes. AKHQ would show the same text for the bytes of "Hello".
            val json = """{"payload":"SGVsbG8="}"""

            val viaPlugin = AvroJson.toObject(json, schema) as GenericData.Record
            assertEquals("SGVsbG8=", String(bytesOf(viaPlugin.get("payload"))))

            val asBase64 = AvroJson.readPlain(json, schema.rawSchema()) as GenericData.Record
            assertEquals("Hello", String(bytesOf(asBase64.get("payload"))))
        }

        @Test
        fun `a wrapped union is read by the plain reader as well`() {
            val json = avroJsonOf(ORDER_RAW, order())

            assertSameAvro(ORDER_RAW, order(), AvroJson.readPlain(json, ORDER_RAW))
        }
    }

    @Nested
    @DisplayName("simple types")
    inner class SimpleTypes {

        @Test
        fun `booleans, numbers and text`() {
            assertEquals(true, plain(primitive("boolean"), "true"))
            assertEquals(5, plain(primitive("int"), "5"))
            assertEquals(5_000_000_000L, plain(primitive("long"), "5000000000"))
            assertEquals(1.5f, plain(primitive("float"), "1.5"))
            assertEquals(1.5, plain(primitive("double"), "1.5"))
            assertEquals("a b", plain(primitive("string"), "\"a b\""))
            assertNull(plain(primitive("null"), "null"))
        }

        @Test
        fun `a whole number may be written with a fraction or an exponent`() {
            assertEquals(5, plain(primitive("int"), "5.0"))
            assertEquals(1000, plain(primitive("int"), "1E3"))
            assertEquals(7L, plain(primitive("long"), "7.00"))
            assertEquals(0, plain(primitive("int"), "0.0"))
            assertEquals(-3, plain(primitive("int"), "-3.0"))
        }

        @Test
        fun `a fraction is not a whole number`() {
            assertEquals("\$: expected an integer, found the number 5.5", plainFailure(primitive("int"), "5.5"))
            assertEquals("\$: expected an integer, found the number 0.001", plainFailure(primitive("long"), "1E-3"))
        }

        @Test
        fun `numbers are checked against the range of their type`() {
            assertEquals(Int.MAX_VALUE, plain(primitive("int"), "2147483647"))
            assertEquals("\$: 2147483648 is too big for an int", plainFailure(primitive("int"), "2147483648"))
            assertEquals(Long.MAX_VALUE, plain(primitive("long"), "9223372036854775807"))
            assertEquals(
                "\$: 9223372036854775808 is too big for a long",
                plainFailure(primitive("long"), "9223372036854775808")
            )
        }

        @Test
        fun `NaN and the infinities are text`() {
            assertEquals(Double.NaN, plain(primitive("double"), "\"NaN\""))
            assertEquals(Double.POSITIVE_INFINITY, plain(primitive("double"), "\"Infinity\""))
            assertEquals(Double.NEGATIVE_INFINITY, plain(primitive("double"), "\"-Infinity\""))
            assertEquals(Float.NaN, plain(primitive("float"), "\"NaN\""))
        }

        @Test
        fun `a value of another kind names what was found`() {
            assertEquals("\$: expected a boolean, found the text \"true\"", plainFailure(primitive("boolean"), "\"true\""))
            assertEquals("\$: expected a string, found the number 5", plainFailure(primitive("string"), "5"))
            assertEquals("\$: expected null, found the boolean false", plainFailure(primitive("null"), "false"))
            assertEquals("\$: expected a double, found an object", plainFailure(primitive("double"), "{}"))
            assertEquals("\$: expected a float, found an array", plainFailure(primitive("float"), "[]"))
        }

        @Test
        fun `long text is cut in the message`() {
            val message = plainFailure(primitive("int"), "\"${"x".repeat(100)}\"")

            assertEquals("\$: expected an integer, found the text \"${"x".repeat(40)}...\"", message)
        }
    }

    @Nested
    @DisplayName("logical types")
    inner class LogicalTypesTest {

        @ParameterizedTest(name = "{0} {1} {2}")
        @CsvSource(
            "int, date, 2024-05-01, 19844",
            "int, date, 1970-01-01, 0",
            "int, date, 1969-12-31, -1",
            "int, time-millis, 10:15:30, 36930000",
            "int, time-millis, 10:15:30.123, 36930123",
            "long, time-micros, 10:15:30.123456, 36930123456",
            "long, timestamp-millis, 2024-05-01T10:15:30.123Z, 1714558530123",
            "long, timestamp-millis, 2024-05-01T12:15:30.123+02:00, 1714558530123",
            "long, timestamp-millis, 1969-12-31T23:59:59.999Z, -1",
            "long, timestamp-micros, 2024-05-01T10:15:30.123456Z, 1714558530123456",
            "long, timestamp-micros, 1969-12-31T23:59:59.999999Z, -1",
            "long, local-timestamp-millis, 2024-05-01T10:15:30.123, 1714558530123",
            "long, local-timestamp-micros, 2024-05-01T10:15:30.123456, 1714558530123456",
        )
        fun `are read from ISO text`(type: String, logical: String, text: String, expected: Long) {
            val value = plain("""{"type":"$type","logicalType":"$logical"}""", "\"$text\"")

            assertEquals(expected, (value as Number).toLong())
            // The serializers need exactly an Integer or a Long, or they cannot find the type in a union.
            assertEquals(if (type == "int") Int::class.javaObjectType else Long::class.javaObjectType, value.javaClass)
        }

        @Test
        fun `are also read from their raw numbers`() {
            assertEquals(19844, plain("""{"type":"int","logicalType":"date"}""", "19844"))
            assertEquals(36930000, plain("""{"type":"int","logicalType":"time-millis"}""", "36930000"))
            assertEquals(
                1714558530123L,
                plain("""{"type":"long","logicalType":"timestamp-millis"}""", "1714558530123")
            )
        }

        @Test
        fun `a uuid is text`() {
            val id = "0f8fad5b-d9cb-469f-a165-70867728950e"
            assertEquals(id, plain("""{"type":"string","logicalType":"uuid"}""", "\"$id\""))
        }

        @Test
        fun `text that is not a date says what is expected`() {
            val date = """{"type":"int","logicalType":"date"}"""
            val timestamp = """{"type":"long","logicalType":"timestamp-millis"}"""
            val time = """{"type":"int","logicalType":"time-millis"}"""

            assertEquals(
                "\$: expected a date like 2024-05-01, found the text \"2024-13-45\"",
                plainFailure(date, "\"2024-13-45\"")
            )
            assertEquals(
                "\$: expected a timestamp like 2024-05-01T10:15:30Z, found the text \"yesterday\"",
                plainFailure(timestamp, "\"yesterday\"")
            )
            assertEquals(
                "\$: expected a time like 10:15:30, found the text \"25:61\"",
                plainFailure(time, "\"25:61\"")
            )
        }

        @Test
        fun `a plain long is not read as a date`() {
            assertEquals(
                "\$: expected an integer, found the text \"2024-05-01\"",
                plainFailure(primitive("int"), "\"2024-05-01\"")
            )
        }
    }

    @Nested
    @DisplayName("bytes, fixed and decimals")
    inner class BinaryTypes {

        private val fixed4 = """{"type":"fixed","name":"F4","size":4}"""
        private val decimalBytes = """{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"""
        private val decimalFixed =
            """{"type":"fixed","name":"D8","size":8,"logicalType":"decimal","precision":10,"scale":2}"""

        @Test
        fun `bytes are base64`() {
            assertArrayEquals("hello".toByteArray(), bytesOf(plain(primitive("bytes"), "\"aGVsbG8=\"")))
            assertArrayEquals(ByteArray(0), bytesOf(plain(primitive("bytes"), "\"\"")))
        }

        @Test
        fun `bytes that are no base64 are the characters of the Avro JSON encoding`() {
            val expected = byteArrayOf('h'.code.toByte(), '!'.code.toByte(), 0xD2.toByte())

            assertArrayEquals(expected, bytesOf(plain(primitive("bytes"), "\"h!\\u00d2\"")))
        }

        @Test
        fun `bytes of a character beyond Latin-1 are refused`() {
            assertEquals(
                "\$: expected text in base64, found the text \"€\"",
                plainFailure(primitive("bytes"), "\"€\"")
            )
        }

        @Test
        fun `fixed is base64 of its size`() {
            assertArrayEquals("abcd".toByteArray(), bytesOf(plain(fixed4, "\"YWJjZA==\"")))
        }

        @Test
        fun `fixed that is no base64 of its size is the characters when they have its size`() {
            assertArrayEquals("abcd".toByteArray(), bytesOf(plain(fixed4, "\"abcd\"")))
        }

        @Test
        fun `fixed of another size is refused`() {
            assertEquals(
                "\$: expected text in base64 with 4 bytes, found the text \"YWJjZGVm\"",
                plainFailure(fixed4, "\"YWJjZGVm\"")
            )
        }

        @Test
        fun `fixed is a value the serializer can write`() {
            val value = plain(fixed4, "\"YWJjZA==\"")

            assertTrue(value is GenericFixed)
            assertEquals("F4", (value as GenericFixed).schema.name)
        }

        @Test
        fun `a decimal is read from a number`() {
            val expected = byteArrayOf(0x04, 0xD2.toByte())

            assertArrayEquals(expected, bytesOf(plain(decimalBytes, "12.34")))
            assertArrayEquals(expected, bytesOf(plain(decimalBytes, "12.340")))
            assertArrayEquals(expected, bytesOf(plain(decimalBytes, "1234E-2")))
            assertArrayEquals(expected, bytesOf(plain(decimalBytes, "0.1234E2")))
            assertArrayEquals(BigInteger.valueOf(-1234).toByteArray(), bytesOf(plain(decimalBytes, "-12.34")))
            assertArrayEquals(byteArrayOf(0), bytesOf(plain(decimalBytes, "0")))
        }

        @Test
        fun `a whole number is a decimal`() {
            // 12 with scale 2 is 1200 = 0x04B0
            assertArrayEquals(byteArrayOf(0x04, 0xB0.toByte()), bytesOf(plain(decimalBytes, "12")))
        }

        @Test
        fun `a decimal is read from text with digits`() {
            assertArrayEquals(byteArrayOf(0x04, 0xD2.toByte()), bytesOf(plain(decimalBytes, "\"12.34\"")))
        }

        @Test
        fun `a decimal that is no number is the raw bytes of the Avro JSON encoding`() {
            assertArrayEquals(byteArrayOf(0x04, 0xD2.toByte()), bytesOf(plain(decimalBytes, "\"\\u0004\\u00d2\"")))
        }

        @Test
        fun `a decimal as fixed is padded to the size`() {
            assertArrayEquals(
                byteArrayOf(0, 0, 0, 0, 0, 0, 0x04, 0xD2.toByte()),
                bytesOf(plain(decimalFixed, "12.34"))
            )
            assertArrayEquals(
                byteArrayOf(-1, -1, -1, -1, -1, -1, 0xFB.toByte(), 0x2E),
                bytesOf(plain(decimalFixed, "-12.34"))
            )
        }

        @Test
        fun `a decimal needing another scale is refused`() {
            val message = plainFailure(decimalBytes, "12.345")

            assertTrue(message.startsWith("\$: "), message)
            assertTrue("scale 3" in message && "scale 2" in message, message)
        }

        @Test
        fun `a decimal with too many digits is refused`() {
            val message = plainFailure(decimalBytes, "123456789012.34")

            assertTrue(message.startsWith("\$: "), message)
            assertTrue("precision 14" in message && "precision 10" in message, message)
        }

        @Test
        fun `a decimal must be a number or bytes`() {
            assertEquals("\$: expected a number, found the boolean true", plainFailure(decimalBytes, "true"))
        }
    }

    @Nested
    @DisplayName("enums, arrays, maps and records")
    inner class Containers {

        @Test
        fun `an enum is its symbol`() {
            val value = plain("""{"type":"enum","name":"Status","symbols":["NEW","PAID"]}""", "\"PAID\"")

            assertTrue(value is GenericData.EnumSymbol)
            assertEquals("PAID", value.toString())
        }

        @Test
        fun `an unknown symbol lists the symbols`() {
            val schema = """{"type":"enum","name":"Status","symbols":["NEW","PAID"]}"""

            assertEquals("\$: expected one of NEW, PAID, found the text \"LOST\"", plainFailure(schema, "\"LOST\""))
        }

        @Test
        fun `a long list of symbols is cut`() {
            val symbols = ('A'..'L').joinToString(",") { "\"S$it\"" }
            val message = plainFailure("""{"type":"enum","name":"Many","symbols":[$symbols]}""", "\"nope\"")

            assertEquals(
                "\$: expected one of SA, SB, SC, SD, SE, SF, SG, SH, SI, SJ, ..., found the text \"nope\"",
                message
            )
        }

        @Test
        fun `arrays and maps`() {
            assertEquals(listOf(1, 2, 3), plain("""{"type":"array","items":"int"}""", "[1,2,3]"))
            assertEquals(emptyList<Any>(), plain("""{"type":"array","items":"int"}""", "[]"))
            assertEquals(
                mapOf("a" to 1L, "b" to 2L),
                plain("""{"type":"map","values":"long"}""", """{"a":1,"b":2}""")
            )
            assertEquals(
                listOf(listOf(1), listOf(2, 3)),
                plain("""{"type":"array","items":{"type":"array","items":"int"}}""", "[[1],[2,3]]")
            )
        }

        @Test
        fun `the path points to the element that does not fit`() {
            val array = """{"type":"array","items":"int"}"""
            val map = """{"type":"map","values":"int"}"""

            assertEquals("\$[1]: expected an integer, found the text \"x\"", plainFailure(array, """[1,"x"]"""))
            assertEquals("\$.b: expected an integer, found the text \"x\"", plainFailure(map, """{"a":1,"b":"x"}"""))
            assertEquals("\$: expected an array, found an object", plainFailure(array, "{}"))
            assertEquals("\$: expected an object, found an array", plainFailure(map, "[]"))
        }

        @Test
        fun `a record is read by field names`() {
            val record = AvroJson.toObject(ORDER_AKHQ, ORDER) as GenericData.Record

            assertEquals(1L, record.get("id"))
            assertEquals("Ada", record.get("customerName").toString())
            assertEquals("PAID", record.get("status").toString())
        }

        @Test
        fun `a field that is left out takes the default of the schema`() {
            val schema = schemaOf(
                """{"type":"record","name":"R","fields":[
                    {"name":"n","type":"int","default":7},
                    {"name":"tags","type":{"type":"array","items":"string"},"default":["x"]},
                    {"name":"s","type":["null","string"]}]}"""
            )

            val record = AvroJson.readPlain("{}", schema) as GenericData.Record

            assertEquals(7, record.get("n"))
            assertEquals(listOf("x"), (record.get("tags") as Collection<*>).map { it.toString() })
            assertNull(record.get("s"))
        }

        @Test
        fun `a field that is left out and has no default is refused`() {
            val message = plainFailure(ORDER_SCHEMA_TEXT, """{"customerName":null}""")

            assertEquals("\$.id: is missing and has no default in the schema", message)
        }

        @Test
        fun `a field that is not in the schema is refused`() {
            val json = ORDER_AKHQ.replace(""""id":1,""", """"id":1,"nope":true,""")

            assertEquals("\$.nope: is not a field of shop.Order", plainFailure(ORDER_SCHEMA_TEXT, json))
        }

        @Test
        fun `the path of a nested field is shown`() {
            val json = ORDER_AKHQ.replace(""""city":"Munich"""", """"city":5""")

            assertEquals("\$.address.city: expected a string, found the number 5", plainFailure(ORDER_SCHEMA_TEXT, json))
        }

        @Test
        fun `a recursive schema is read as deep as the text goes`() {
            val schema = """{"type":"record","name":"Node","fields":[
                {"name":"value","type":"int"},
                {"name":"next","type":["null","Node"],"default":null}]}"""

            val node = plain(schema, """{"value":1,"next":{"value":2,"next":{"value":3}}}""") as GenericData.Record
            val third = ((node.get("next") as GenericData.Record).get("next") as GenericData.Record)

            assertEquals(3, third.get("value"))
            assertNull(third.get("next"))
        }
    }

    @Nested
    @DisplayName("unions")
    inner class Unions {

        @Test
        fun `a nullable value is plain or null`() {
            val schema = """["null","string"]"""

            assertNull(plain(schema, "null"))
            assertEquals("x", plain(schema, "\"x\""))
        }

        @Test
        fun `a nullable value may be wrapped like in the Avro JSON encoding`() {
            assertEquals("x", plain("""["null","string"]""", """{"string":"x"}"""))
        }

        @Test
        fun `the types of a union are tried in order`() {
            val schema = """["null","int","string"]"""

            assertEquals(5, plain(schema, "5"))
            assertEquals("5", plain(schema, "\"5\""))
            assertNull(plain(schema, "null"))
            assertEquals(5, plain(schema, """{"int":5}"""))
        }

        @Test
        fun `a number goes to the first type that holds it`() {
            val schema = """["long","double"]"""

            assertEquals(2L, plain(schema, "2"))
            assertEquals(1.5, plain(schema, "1.5"))
            assertEquals(1.5, plain(schema, """{"double":1.5}"""))
        }

        @Test
        fun `records are told apart by their fields`() {
            val schema = """[
                {"type":"record","name":"A","namespace":"p","fields":[{"name":"a","type":"int"}]},
                {"type":"record","name":"B","namespace":"p","fields":[{"name":"b","type":"string"}]}]"""

            assertEquals("p.A", (plain(schema, """{"a":1}""") as GenericData.Record).schema.fullName)
            assertEquals("p.B", (plain(schema, """{"b":"x"}""") as GenericData.Record).schema.fullName)
        }

        @Test
        fun `records can be wrapped by their full or their short name`() {
            val schema = """[
                {"type":"record","name":"A","namespace":"p","fields":[{"name":"a","type":"int"}]},
                {"type":"record","name":"B","namespace":"p","fields":[{"name":"b","type":"string"}]}]"""

            assertEquals("p.B", (plain(schema, """{"p.B":{"b":"x"}}""") as GenericData.Record).schema.fullName)
            assertEquals("p.B", (plain(schema, """{"B":{"b":"x"}}""") as GenericData.Record).schema.fullName)
        }

        @Test
        fun `a map with one entry is not taken for a wrapped value`() {
            val schema = """["null",{"type":"map","values":"string"}]"""

            assertEquals(mapOf("k" to "v"), plain(schema, """{"k":"v"}"""))
            assertEquals(mapOf("k" to "v"), plain(schema, """{"map":{"k":"v"}}"""))
            assertEquals(mapOf("map" to "x"), plain(schema, """{"map":"x"}"""))
        }

        @Test
        fun `an array is told from a nullable value`() {
            assertEquals(listOf("a"), plain("""["null",{"type":"array","items":"string"}]""", """["a"]"""))
        }

        @Test
        fun `the message of a union says what it expected`() {
            val message = plainFailure("""["null","int","string"]""", "true")

            assertEquals("\$: expected one of (null, int, string), found the boolean true", message)
        }

        @Test
        fun `a union with logical types names them`() {
            val message = plainFailure(
                """["null",{"type":"long","logicalType":"timestamp-millis"}]""",
                "true"
            )

            assertEquals("\$: expected one of (null, timestamp-millis), found the boolean true", message)
        }

        @Test
        fun `the message names the field when no type of a union holds the value`() {
            val json = ORDER_AKHQ.replace(""""zip":"80331"""", """"zip":5""")

            assertEquals(
                "\$.address.zip: expected one of (null, string), found the number 5",
                plainFailure(ORDER_SCHEMA_TEXT, json)
            )
        }

        @Test
        fun `of two records the error is the one of the record that has the fields`() {
            val schema = """[
                {"type":"record","name":"A","namespace":"p","fields":[{"name":"a","type":"int"}]},
                {"type":"record","name":"B","namespace":"p","fields":[{"name":"b","type":"string"}]}]"""

            assertEquals("\$.b: expected a string, found the number 5", plainFailure(schema, """{"b":5}"""))
            assertEquals("\$.a: expected an integer, found the text \"x\"", plainFailure(schema, """{"a":"x"}"""))
        }

        @Test
        fun `a missing field is reported when no other type got further`() {
            val json = ORDER_AKHQ.replace(""""city":"Munich",""", "")

            assertEquals("\$.address.city: is missing and has no default in the schema", plainFailure(ORDER_SCHEMA_TEXT, json))
        }
    }

    @Nested
    @DisplayName("text that is no value")
    inner class Failures {

        @Test
        fun `invalid JSON tells where it is wrong`() {
            val message = plainFailure(ORDER_SCHEMA_TEXT, """{"id":""")

            assertTrue(message.startsWith("The text is not valid JSON: "), message)
            assertTrue("line 1" in message, message)
        }

        @Test
        fun `text after the value is refused`() {
            val message = plainFailure(primitive("int"), "1 2")

            assertTrue(message.startsWith("The text is not valid JSON: "), message)
        }

        @Test
        fun `empty text is refused`() {
            assertEquals("The text is empty", plainFailure(primitive("int"), ""))
            assertEquals("The text is empty", plainFailure(primitive("int"), "  \n "))
        }

        @Test
        fun `when both notations fail the message is the one of the plain reader and keeps the other`() {
            val json = ORDER_AKHQ.replace(""""id":1""", """"id":"abc"""")

            val failure = assertThrows<SerializationException> { AvroJson.toObject(json, ORDER) }

            assertEquals("\$.id: expected an integer, found the text \"abc\"", failure.message)
            // What Avro said about the same text stays available for whoever needs to see it.
            assertEquals(1, failure.suppressed.size)
            assertTrue(!failure.suppressed[0].message.isNullOrBlank())
        }

        @Test
        @Timeout(10)
        fun `a number with a huge exponent is refused at once`() {
            assertEquals(
                "\$: the number 1E+999999999 is out of range",
                plainFailure(primitive("int"), "1E999999999")
            )
            assertEquals(
                "\$: the number 1E-999999999 is out of range",
                plainFailure(primitive("long"), "1E-999999999")
            )
            val decimal = """{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"""
            assertEquals("\$: the number 1E+999999999 is out of range", plainFailure(decimal, "1E999999999"))
            assertEquals(
                "\$: the text \"1E-999999999\" is out of range",
                plainFailure(decimal, "\"1E-999999999\"")
            )
        }
    }

    @Nested
    @DisplayName("generated messages")
    inner class GeneratedMessages {

        private val json = JsonNodeFactory.instance
        private val decimals = Conversions.DecimalConversion()

        // What AKHQ shows for a value of the schema: plain unions, ISO text for dates and times, base64 for bytes, a
        // number for a decimal. Null fields can be left out, as AKHQ does with its NON_NULL setting.
        private fun akhq(value: Any?, schema: Schema, omitNulls: Boolean): JsonNode {
            if (value == null) return json.nullNode()
            return when (schema.type) {
                Schema.Type.UNION -> akhq(value, schema.types[GenericData.get().resolveUnion(schema, value)], omitNulls)
                Schema.Type.RECORD -> json.objectNode().also { node ->
                    val record = value as GenericData.Record
                    for (field in schema.fields) {
                        val child = record.get(field.pos())
                        if (child != null || !omitNulls) node.set<JsonNode>(field.name(), akhq(child, field.schema(), omitNulls))
                    }
                }

                Schema.Type.ARRAY -> json.arrayNode().also { node ->
                    (value as Collection<*>).forEach { node.add(akhq(it, schema.elementType, omitNulls)) }
                }

                Schema.Type.MAP -> json.objectNode().also { node ->
                    (value as Map<*, *>).forEach { (key, child) ->
                        node.set<JsonNode>(key.toString(), akhq(child, schema.valueType, omitNulls))
                    }
                }

                Schema.Type.ENUM, Schema.Type.STRING -> json.textNode(value.toString())
                Schema.Type.BOOLEAN -> json.booleanNode(value as Boolean)
                Schema.Type.FLOAT -> FloatNode.valueOf(value as Float)
                Schema.Type.DOUBLE -> DoubleNode.valueOf(value as Double)
                Schema.Type.BYTES -> binary(schema, bytesOf(value)) { decimals.fromBytes(value as ByteBuffer, schema, it) }
                Schema.Type.FIXED -> binary(schema, bytesOf(value)) { decimals.fromFixed(value as GenericFixed, schema, it) }
                Schema.Type.INT -> integer(schema, value as Int)
                Schema.Type.LONG -> long(schema, value as Long)
                Schema.Type.NULL -> json.nullNode()
            }
        }

        private fun integer(schema: Schema, number: Int): JsonNode = when (schema.logicalType?.name) {
            "date" -> text(DateTimeFormatter.ISO_LOCAL_DATE, LocalDate.ofEpochDay(number.toLong()))
            "time-millis" ->
                if (number in 0 until 86_400_000) text(DateTimeFormatter.ISO_LOCAL_TIME, LocalTime.ofNanoOfDay(number * 1_000_000L))
                else IntNode.valueOf(number)

            else -> IntNode.valueOf(number)
        }

        private fun long(schema: Schema, number: Long): JsonNode = when (schema.logicalType?.name) {
            "timestamp-millis" -> text(DateTimeFormatter.ISO_INSTANT, Instant.ofEpochMilli(number))
            "timestamp-micros" -> text(DateTimeFormatter.ISO_INSTANT, instantOfMicros(number))
            "local-timestamp-millis" ->
                text(DateTimeFormatter.ISO_LOCAL_DATE_TIME, LocalDateTime.ofInstant(Instant.ofEpochMilli(number), ZoneOffset.UTC))

            "local-timestamp-micros" ->
                text(DateTimeFormatter.ISO_LOCAL_DATE_TIME, LocalDateTime.ofInstant(instantOfMicros(number), ZoneOffset.UTC))

            "time-micros" ->
                if (number in 0 until 86_400_000_000L) text(DateTimeFormatter.ISO_LOCAL_TIME, LocalTime.ofNanoOfDay(number * 1_000L))
                else LongNode.valueOf(number)

            else -> LongNode.valueOf(number)
        }

        private fun text(formatter: DateTimeFormatter, value: TemporalAccessor): JsonNode = json.textNode(formatter.format(value))

        private fun binary(schema: Schema, bytes: ByteArray, decimal: (LogicalType) -> BigDecimal): JsonNode =
            schema.logicalType?.takeIf { it.name == "decimal" }?.let { DecimalNode.valueOf(decimal(it)) }
                ?: json.textNode(Base64.getEncoder().encodeToString(bytes))

        private fun instantOfMicros(micros: Long) =
            Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L)

        private fun roundTrip(schemaText: String, iterations: Int = 300) {
            val avro = AvroSchema(schemaText)
            val schema = avro.rawSchema()

            repeat(iterations) {
                val avroJson = AvroGenerator.generateAvroMessage(null, avro)
                val message = AvroSchemaUtils.toObject(avroJson, avro)

                assertSameAvro(schema, message, AvroJson.toObject(avroJson, avro))

                for (omitNulls in listOf(false, true)) {
                    val akhqJson = akhq(message, schema, omitNulls).toString()
                    val read = try {
                        AvroJson.readPlain(akhqJson, schema)
                    } catch (e: Exception) {
                        fail("Cannot read $akhqJson, which is $avroJson as AKHQ shows it", e)
                    }
                    assertArrayEquals(encode(schema, message), encode(schema, read)) {
                        "$akhqJson (from $avroJson) was read as $read"
                    }
                }
            }
        }

        @Test
        fun `orders are read back as they were`() = roundTrip(ORDER_SCHEMA_TEXT)

        @Test
        fun `every kind of value is read back as it was`() = roundTrip(
            """
            {"type":"record","name":"Sink","namespace":"t","fields":[
              {"name":"b","type":"boolean"},
              {"name":"i","type":"int"},
              {"name":"l","type":"long"},
              {"name":"f","type":"float"},
              {"name":"d","type":"double"},
              {"name":"s","type":"string"},
              {"name":"by","type":"bytes"},
              {"name":"fx","type":{"type":"fixed","name":"Fx","size":4}},
              {"name":"decFixed","type":{"type":"fixed","name":"DecFx","size":8,"logicalType":"decimal","precision":10,"scale":3}},
              {"name":"decBytes","type":{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}},
              {"name":"uuid","type":{"type":"string","logicalType":"uuid"}},
              {"name":"date","type":{"type":"int","logicalType":"date"}},
              {"name":"timeMillis","type":{"type":"int","logicalType":"time-millis"}},
              {"name":"timeMicros","type":{"type":"long","logicalType":"time-micros"}},
              {"name":"tsMillis","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"tsMicros","type":{"type":"long","logicalType":"timestamp-micros"}},
              {"name":"localMillis","type":{"type":"long","logicalType":"local-timestamp-millis"}},
              {"name":"localMicros","type":{"type":"long","logicalType":"local-timestamp-micros"}},
              {"name":"map","type":{"type":"map","values":["null","long"]}},
              {"name":"nested","type":{"type":"array","items":{"type":"array","items":"int"}}},
              {"name":"choice","type":["null","int","string",
                {"type":"record","name":"Pair","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}]},
              {"name":"maybeEnum","type":["null",{"type":"enum","name":"Color","symbols":["RED","GREEN"]}],"default":null}
            ]}
            """
        )

        @Test
        fun `values in the usual range are read back as they were`() = roundTrip(
            """
            {"type":"record","name":"Usual","namespace":"t","fields":[
              {"name":"date","type":{"type":"int","logicalType":"date","arg.properties":{"range":{"min":0,"max":40000}}}},
              {"name":"timeMillis","type":{"type":"int","logicalType":"time-millis","arg.properties":{"range":{"min":0,"max":86399999}}}},
              {"name":"timeMicros","type":{"type":"long","logicalType":"time-micros","arg.properties":{"range":{"min":0,"max":86399999999}}}},
              {"name":"tsMillis","type":{"type":"long","logicalType":"timestamp-millis","arg.properties":{"range":{"min":0,"max":4102444800000}}}},
              {"name":"tsMicros","type":{"type":"long","logicalType":"timestamp-micros","arg.properties":{"range":{"min":0,"max":4102444800000000}}}},
              {"name":"localMillis","type":{"type":"long","logicalType":"local-timestamp-millis","arg.properties":{"range":{"min":0,"max":4102444800000}}}},
              {"name":"localMicros","type":{"type":"long","logicalType":"local-timestamp-micros","arg.properties":{"range":{"min":0,"max":4102444800000000}}}},
              {"name":"maybeTime","type":["null",{"type":"long","logicalType":"timestamp-millis","arg.properties":{"range":{"min":0,"max":4102444800000}}}],"default":null}
            ]}
            """
        )

        // The ends of the range, which a random message rarely has.
        @Test
        fun `a decimal as fixed is read back as it was`() {
            val schema = schemaOf("""{"type":"fixed","name":"D8","size":8,"logicalType":"decimal","precision":10,"scale":3}""")

            for (value in listOf("0", "1.5", "-12345.678", "9999999.999", "-9999999.999")) {
                val fixed = decimals.toFixed(BigDecimal(value), schema, schema.logicalType)

                val read = AvroJson.readPlain(akhq(fixed, schema, omitNulls = false).toString(), schema)

                assertArrayEquals(bytesOf(fixed), bytesOf(read), value)
            }
        }
    }
}
