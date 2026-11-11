package io.confluent.intellijplugin.registry

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.kafka.common.errors.SerializationException
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.Base64

/**
 * Reads the JSON of an Avro value for the producer.
 *
 * Avro data is written in JSON in two ways, and people paste both:
 *  - The Avro JSON encoding, which Avro's own decoder reads and which the consumer shows. A union value is wrapped in
 *    an object named after its type, like `{"string": "Ada"}`, and a date or a decimal is shown as its raw number.
 *  - Plain JSON, which tools like AKHQ show: the value itself (`"Ada"`), ISO-8601 text for dates and timestamps, a
 *    number for a decimal, base64 for bytes, and nothing at all for a null that is left out.
 *
 * [toObject] reads the Avro JSON encoding first, so whatever was accepted before is read as before, and reads plain
 * JSON when that fails. The plain reader understands the wrapped unions as well, so when the text is neither, its
 * message tells where it goes wrong.
 */
object AvroJson {

    private val mapper = ObjectMapper()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    /** @throws SerializationException when [json] is not a value of [schema] in either notation. */
    fun toObject(json: String, schema: AvroSchema): Any? = try {
        AvroSchemaUtils.toObject(json, schema)
    } catch (avroJsonFailure: Exception) {
        try {
            readPlain(json, schema.rawSchema())
        } catch (plainFailure: SerializationException) {
            plainFailure.addSuppressed(avroJsonFailure)
            throw plainFailure
        }
    }

    internal fun readPlain(json: String, schema: Schema): Any? {
        val root = try {
            mapper.readTree(json)
        } catch (e: JsonProcessingException) {
            val at = e.location?.let { " (line ${it.lineNr}, column ${it.columnNr})" } ?: ""
            throw SerializationException("The text is not valid JSON: ${e.originalMessage}$at", e)
        }
        if (root == null || root.isMissingNode)
            throw SerializationException("The text is empty")

        return try {
            PlainReader.read(root, schema, "$")
        } catch (e: Mismatch) {
            throw SerializationException(e.message, e)
        }
    }

    /**
     * Thrown for a value that does not fit its schema, without a stack trace as a union tries its types in turn.
     * It knows how far into the value it got: an error inside a field is further than an error about the fields of a
     * record, which in turn is further than a value of the wrong kind.
     */
    private class Mismatch(val path: String, reason: String, shape: Boolean = false) :
        RuntimeException("$path: $reason", null, false, false) {
        val depth = 2 * levelOf(path) - if (shape) 1 else 0
    }

    private fun levelOf(path: String) = path.count { it == '.' || it == '[' }

    private object PlainReader {
        private const val MAX_SHOWN = 40
        private const val MAX_SYMBOLS_SHOWN = 10
        private const val MAX_EXPONENT = 1_000
        private val decimals = Conversions.DecimalConversion()

        fun read(node: JsonNode, schema: Schema, path: String): Any? = when (schema.type) {
            Schema.Type.UNION -> union(node, schema, path)
            Schema.Type.NULL -> if (node.isNull) null else mismatch(path, "null", node)
            Schema.Type.BOOLEAN -> if (node.isBoolean) node.booleanValue() else mismatch(path, "a boolean", node)
            Schema.Type.INT -> int(node, schema, path)
            Schema.Type.LONG -> long(node, schema, path)
            Schema.Type.FLOAT -> double(node, path, "a float").toFloat()
            Schema.Type.DOUBLE -> double(node, path, "a double")
            Schema.Type.STRING -> if (node.isTextual) node.textValue() else mismatch(path, "a string", node)
            Schema.Type.BYTES -> bytes(node, schema, path)
            Schema.Type.FIXED -> fixed(node, schema, path)
            Schema.Type.ENUM -> enum(node, schema, path)
            Schema.Type.ARRAY -> array(node, schema, path)
            Schema.Type.MAP -> map(node, schema, path)
            Schema.Type.RECORD -> record(node, schema, path)
        }

        private fun union(node: JsonNode, schema: Schema, path: String): Any? {
            val failures = mutableListOf<Mismatch>()

            // The Avro JSON encoding: {"typeName": value}. It is tried first, but a map of one entry may look the same.
            if (node.isObject && node.size() == 1) {
                val name = node.fieldNames().next()
                val wrapped = schema.types.firstOrNull { it.type != Schema.Type.NULL && (it.fullName == name || it.name == name) }
                if (wrapped != null) {
                    try {
                        return read(node.get(name), wrapped, path)
                    } catch (e: Mismatch) {
                        failures += e
                    }
                }
            }

            for (type in schema.types) {
                try {
                    return read(node, type, path)
                } catch (e: Mismatch) {
                    failures += e
                }
            }
            throw unionMismatch(node, schema, path, failures)
        }

        // A type that got further into the value than just its kind is the one the value was meant for.
        private fun unionMismatch(node: JsonNode, schema: Schema, path: String, failures: List<Mismatch>): Mismatch {
            val deepest = failures.maxByOrNull { it.depth }
            if (deepest != null && deepest.depth > 2 * levelOf(path))
                return deepest
            val types = schema.types.joinToString(", ") { it.logicalType?.name ?: it.fullName }
            return Mismatch(path, "expected one of ($types), found ${describe(node)}")
        }

        private fun record(node: JsonNode, schema: Schema, path: String): GenericData.Record {
            if (!node.isObject) mismatch(path, "an object for ${schema.fullName}", node)

            val known = schema.fields.map { it.name() }.toSet()
            node.fieldNames().forEach {
                if (it !in known) throw Mismatch("$path.$it", "is not a field of ${schema.fullName}", shape = true)
            }

            val record = GenericData.Record(schema)
            for (field in schema.fields) {
                val fieldPath = "$path.${field.name()}"
                val child = node.get(field.name())
                record.put(
                    field.pos(),
                    when {
                        child != null -> read(child, field.schema(), fieldPath)
                        field.hasDefaultValue() -> GenericData.get().getDefaultValue(field)
                        field.schema().isNullable -> null
                        else -> throw Mismatch(fieldPath, "is missing and has no default in the schema", shape = true)
                    }
                )
            }
            return record
        }

        private fun array(node: JsonNode, schema: Schema, path: String): GenericData.Array<Any?> {
            if (!node.isArray) mismatch(path, "an array", node)
            val array = GenericData.Array<Any?>(node.size(), schema)
            node.forEachIndexed { index, element -> array.add(read(element, schema.elementType, "$path[$index]")) }
            return array
        }

        private fun map(node: JsonNode, schema: Schema, path: String): Map<String, Any?> {
            if (!node.isObject) mismatch(path, "an object", node)
            val map = LinkedHashMap<String, Any?>()
            node.fieldNames().forEach { key -> map[key] = read(node.get(key), schema.valueType, "$path.$key") }
            return map
        }

        private fun enum(node: JsonNode, schema: Schema, path: String): GenericData.EnumSymbol {
            if (node.isTextual && schema.hasEnumSymbol(node.textValue()))
                return GenericData.EnumSymbol(schema, node.textValue())
            val symbols = schema.enumSymbols
            val shown = symbols.take(MAX_SYMBOLS_SHOWN).joinToString(", ") + if (symbols.size > MAX_SYMBOLS_SHOWN) ", ..." else ""
            mismatch(path, "one of $shown", node)
        }

        private fun int(node: JsonNode, schema: Schema, path: String): Int {
            if (node.isTextual) {
                when (schema.logicalType?.name) {
                    "date" -> return parsing(path, "a date like 2024-05-01", node) {
                        Math.toIntExact(LocalDate.parse(it).toEpochDay())
                    }

                    "time-millis" -> return parsing(path, "a time like 10:15:30", node) {
                        (LocalTime.parse(it).toNanoOfDay() / 1_000_000).toInt()
                    }
                }
            }
            val value = integral(node, path) ?: mismatch(path, "an integer", node)
            return try {
                value.intValueExact()
            } catch (_: ArithmeticException) {
                throw Mismatch(path, "$value is too big for an int")
            }
        }

        private fun long(node: JsonNode, schema: Schema, path: String): Long {
            if (node.isTextual) {
                val parsed: Long? = when (schema.logicalType?.name) {
                    "timestamp-millis" -> parsing(path, TIMESTAMP, node) { instantOf(it).toEpochMilli() }
                    "timestamp-micros" -> parsing(path, TIMESTAMP, node) { micros(instantOf(it)) }
                    "local-timestamp-millis" -> parsing(path, LOCAL_TIMESTAMP, node) {
                        localDateTimeOf(it).toInstant(ZoneOffset.UTC).toEpochMilli()
                    }

                    "local-timestamp-micros" -> parsing(path, LOCAL_TIMESTAMP, node) {
                        micros(localDateTimeOf(it).toInstant(ZoneOffset.UTC))
                    }

                    "time-micros" -> parsing(path, "a time like 10:15:30", node) {
                        LocalTime.parse(it).toNanoOfDay() / 1_000
                    }

                    else -> null
                }
                if (parsed != null) return parsed
            }
            val value = integral(node, path) ?: mismatch(path, "an integer", node)
            return try {
                value.longValueExact()
            } catch (_: ArithmeticException) {
                throw Mismatch(path, "$value is too big for a long")
            }
        }

        private fun double(node: JsonNode, path: String, expected: String): Double = when {
            node.isNumber -> node.doubleValue()
            node.isTextual && node.textValue() == "NaN" -> Double.NaN
            node.isTextual && node.textValue() == "Infinity" -> Double.POSITIVE_INFINITY
            node.isTextual && node.textValue() == "-Infinity" -> Double.NEGATIVE_INFINITY
            else -> mismatch(path, expected, node)
        }

        private fun bytes(node: JsonNode, schema: Schema, path: String): ByteBuffer {
            val decimal = schema.logicalType as? LogicalTypes.Decimal
            if (decimal != null) {
                decimalOf(node, path)?.let { return encoding(path) { decimals.toBytes(it, schema, decimal) } }
                // Not a number: the raw bytes of the Avro JSON encoding.
                return ByteBuffer.wrap(rawBytes(node, path, "a number", base64 = false) { true })
            }
            return ByteBuffer.wrap(rawBytes(node, path, "text in base64", base64 = true) { true })
        }

        private fun fixed(node: JsonNode, schema: Schema, path: String): GenericFixed {
            val decimal = schema.logicalType as? LogicalTypes.Decimal
            if (decimal != null) {
                decimalOf(node, path)?.let { return encoding(path) { decimals.toFixed(it, schema, decimal) } }
            }
            val bytes = rawBytes(node, path, "text in base64 with ${schema.fixedSize} bytes", base64 = true) {
                it.size == schema.fixedSize
            }
            return GenericData.Fixed(schema, bytes)
        }

        // The bytes as base64, which is what AKHQ shows, or else as the characters of the Avro JSON encoding.
        private fun rawBytes(
            node: JsonNode,
            path: String,
            expected: String,
            base64: Boolean,
            fits: (ByteArray) -> Boolean
        ): ByteArray {
            if (!node.isTextual) mismatch(path, expected, node)
            val text = node.textValue()
            val candidates = sequence {
                if (base64) {
                    try {
                        yield(Base64.getDecoder().decode(text))
                    } catch (_: IllegalArgumentException) {
                    }
                }
                if (text.all { it.code <= 0xFF }) yield(text.toByteArray(Charsets.ISO_8859_1))
            }
            return candidates.firstOrNull(fits) ?: mismatch(path, expected, node)
        }

        private fun decimalOf(node: JsonNode, path: String): BigDecimal? {
            val value = when {
                node.isNumber -> node.decimalValue()
                node.isTextual -> node.textValue().trim().toBigDecimalOrNull()
                else -> null
            } ?: return null
            // BigDecimal needs minutes and gigabytes to rescale a number like 1E999999999.
            if (value.scale() !in -MAX_EXPONENT..MAX_EXPONENT)
                throw Mismatch(path, "${describe(node)} is out of range")
            return value
        }

        // A whole number, also when it is written as 5.0 or 1E3.
        private fun integral(node: JsonNode, path: String): BigInteger? = when {
            node.isIntegralNumber -> node.bigIntegerValue()
            node.isNumber -> decimalOf(node, path)?.stripTrailingZeros()?.takeIf { it.scale() <= 0 }?.toBigIntegerExact()
            else -> null
        }

        private fun instantOf(text: String): Instant = try {
            Instant.parse(text)
        } catch (_: DateTimeParseException) {
            when (val parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(text, ZonedDateTime::from, LocalDateTime::from)) {
                is ZonedDateTime -> parsed.toInstant()
                else -> (parsed as LocalDateTime).toInstant(ZoneOffset.UTC)
            }
        }

        private fun localDateTimeOf(text: String): LocalDateTime =
            DateTimeFormatter.ISO_DATE_TIME.parse(text, LocalDateTime::from)

        private fun micros(instant: Instant): Long =
            Math.addExact(Math.multiplyExact(instant.epochSecond, 1_000_000L), instant.nano / 1_000L)

        private inline fun <T> parsing(path: String, expected: String, node: JsonNode, parse: (String) -> T): T = try {
            parse(node.textValue())
        } catch (_: DateTimeParseException) {
            mismatch(path, expected, node)
        } catch (_: ArithmeticException) {
            throw Mismatch(path, "${node.textValue()} is out of range")
        }

        private inline fun <T> encoding(path: String, encode: () -> T): T = try {
            encode()
        } catch (e: org.apache.avro.AvroTypeException) {
            throw Mismatch(path, e.message ?: "does not fit the decimal type")
        } catch (e: ArithmeticException) {
            throw Mismatch(path, e.message ?: "does not fit the decimal type")
        }

        private fun mismatch(path: String, expected: String, found: JsonNode): Nothing =
            throw Mismatch(path, "expected $expected, found ${describe(found)}")

        private fun describe(node: JsonNode): String = when {
            node.isNull -> "null"
            node.isBoolean -> "the boolean ${node.booleanValue()}"
            node.isNumber -> "the number ${node.asText()}"
            node.isTextual -> "the text \"${node.textValue().take(MAX_SHOWN)}${if (node.textValue().length > MAX_SHOWN) "..." else ""}\""
            node.isArray -> "an array"
            node.isObject -> "an object"
            else -> "something else"
        }

        private const val TIMESTAMP = "a timestamp like 2024-05-01T10:15:30Z"
        private const val LOCAL_TIMESTAMP = "a timestamp like 2024-05-01T10:15:30"
    }
}
