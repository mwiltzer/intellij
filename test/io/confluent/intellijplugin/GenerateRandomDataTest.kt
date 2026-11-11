package io.confluent.intellijplugin

import io.confluent.intellijplugin.util.generator.AvroGenerator
import io.confluent.intellijplugin.util.generator.JsonSchemaGenerator
import io.confluent.intellijplugin.util.generator.PrimitivesGenerator
import io.confluent.intellijplugin.util.generator.ProtobufGenerator
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils
import org.apache.avro.Conversions
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class GenerateRandomDataTest {
    @Test
    fun testGenerateLong() {
        val long1 = PrimitivesGenerator.generateLong()
        val long2 = PrimitivesGenerator.generateLong()
        assertNotEquals(long1, long2)
    }

    @Test
    fun testGenerateDouble() {
        val double1 = PrimitivesGenerator.generateDouble()
        val double2 = PrimitivesGenerator.generateDouble()
        assertNotEquals(double1, double2)
    }

    @Test
    fun testGenerateFloat() {
        val float1 = PrimitivesGenerator.generateFloat()
        val float2 = PrimitivesGenerator.generateFloat()
        assertNotEquals(float1, float2)
    }

    @Test
    fun testGenerateBytes() {
        val bytes1 = PrimitivesGenerator.generateBytesBase64()
        assertDoesNotThrow { Base64.getDecoder().decode(bytes1) }

        val bytes2 = PrimitivesGenerator.generateBytesBase64()
        assertDoesNotThrow { Base64.getDecoder().decode(bytes2) }
        assertNotEquals(bytes1, bytes2)
    }

    @Test
    fun testGenerateAvro() {
        assertDoesNotThrow {
            val avroSchema1 = AvroSchema(avroSchema1)
            val generatedJson1 = AvroGenerator.generateAvroMessage(null, avroSchema1)

            assertNotEquals("", generatedJson1)
            assertNotNull(AvroSchemaUtils.toObject(generatedJson1, avroSchema1))
        }

        assertDoesNotThrow {
            val avroSchema2 = AvroSchema(avroSchema2)
            val generatedJson2 = AvroGenerator.generateAvroMessage(null, avroSchema2)

            assertNotEquals("", generatedJson2)
            assertNotNull(AvroSchemaUtils.toObject(generatedJson2, avroSchema2))
        }
    }

    @Test
    fun testGenerateAvroDecimalAsFixed() {
        // The size of the fixed is bigger than the digits of the decimal need, and the value is negative half of the time.
        val schema = AvroSchema(
            """{"type":"record","name":"Money","fields":[
                {"name":"amount","type":{"type":"fixed","name":"Amount","size":8,"logicalType":"decimal","precision":10,"scale":3}},
                {"name":"tight","type":{"type":"fixed","name":"Tight","size":5,"logicalType":"decimal","precision":10,"scale":0}}]}"""
        )

        repeat(200) {
            val generated = AvroGenerator.generateAvroMessage(null, schema)

            assertNotEquals("", generated)
            val record = AvroSchemaUtils.toObject(generated, schema) as GenericRecord
            assertEquals(8, (record.get("amount") as GenericFixed).bytes().size)
            assertEquals(5, (record.get("tight") as GenericFixed).bytes().size)
        }
    }

    @Test
    fun testGenerateAvroDecimalAsFixedKeepsItsValue() {
        val schema = AvroSchema(
            """{"type":"fixed","name":"Amount","size":8,"logicalType":"decimal","precision":10,"scale":3}"""
        )
        val decimals = Conversions.DecimalConversion()
        val type = schema.rawSchema().logicalType

        repeat(200) {
            val fixed = AvroSchemaUtils.toObject(AvroGenerator.generateAvroMessage(null, schema), schema) as GenericFixed

            // Sign extension keeps the number, so it is the number a decimal of 10 digits can be.
            val value = decimals.fromFixed(fixed, schema.rawSchema(), type)
            assertTrue(value.precision() <= 10, value.toString())
            assertEquals(3, value.scale())
        }
    }

    @Test
    fun testGenerateProtobuf() {
        assertDoesNotThrow {
            val schema1 = ProtobufSchema(protobufSchema1)
            val generated1 = ProtobufGenerator.generateProtobufMessage(null, schema1)

            assertNotEquals("", generated1)
            assertNotNull(ProtobufSchemaUtils.toObject(generated1, schema1))
        }

        assertDoesNotThrow {
            val schema2 = ProtobufSchema(protobufSchema2)
            val generated2 = ProtobufGenerator.generateProtobufMessage(null, schema2)

            assertNotEquals("", generated2)
            assertNotNull(ProtobufSchemaUtils.toObject(generated2, schema2))
        }
    }

    @Test
    fun testGenerateJson() {
        assertDoesNotThrow {
            val schema1 = JsonSchema(jsonSchema1)
            val generated1 = JsonSchemaGenerator.generateJsonMessage(null, schema1, isConfluent = true)

            assertNotEquals("", generated1)
            assertNotNull(JsonSchemaUtils.toObject(generated1, schema1))
        }

        assertDoesNotThrow {
            val schema2 = JsonSchema(jsonSchema2)
            val generated2 = JsonSchemaGenerator.generateJsonMessage(null, schema2, isConfluent = true)

            assertNotEquals("", generated2)
            assertNotNull(JsonSchemaUtils.toObject(generated2, schema2))
        }
    }

    private val avroSchema1 =
        "{\"type\":\"record\",\"name\":\"options_test_record\",\"fields\":[{\"name\":\"array_field\",\"type\":" +
                "{\"type\":\"array\",\"items\":\"string\",\"arg.properties\":{\"options\":[[\"Hello\",\"world\"]," +
                "[\"Goodbye\",\"world\"],[\"We\",\"meet\",\"again\",\"world\"]]}}},{\"name\":\"enum_field\",\"type\":" +
                "{\"type\":\"enum\",\"name\":\"enum_test\",\"symbols\":[\"HELLO\",\"HI_THERE\",\"GREETINGS\"," +
                "\"SALUTATIONS\",\"GOODBYE\"],\"arg.properties\":{\"options\":[\"HELLO\",\"SALUTATIONS\"]}}},{\"name\":" +
                "\"fixed_field\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed_test\",\"size\":2,\"arg.properties\":" +
                "{\"options\":[\"42\",\"EE\"]}}},{\"name\":\"map_field\",\"type\":{\"type\":\"map\",\"values\":\"long\"," +
                "\"arg.properties\":{\"options\":[{\"zero\":0},{\"one\":1,\"two\":2},{\"three\":3,\"four\":4,\"five\":5}," +
                "{\"six\":6,\"seven\":7,\"eight\":8,\"nine\":9}]}}},{\"name\":\"map_key_field\",\"type\":{\"type\":\"map\"," +
                "\"values\":{\"type\":\"int\",\"arg.properties\":{\"options\":[-1,0,1]}},\"arg.properties\":{\"length\":10," +
                "\"keys\":{\"options\":[\"negative\",\"zero\",\"positive\"]}}}},{\"name\":\"record_field\",\"type\":" +
                "{\"type\":\"record\",\"name\":\"record_test\",\"fields\":[{\"name\":\"month\",\"type\":\"string\"}," +
                "{\"name\":\"day\",\"type\":\"int\"}],\"arg.properties\":{\"options\":[{\"month\":\"January\",\"day\":2}," +
                "{\"month\":\"NANuary\",\"day\":0}]}}},{\"name\":\"union_field\",\"type\":[\"null\",{\"type\":\"boolean\"" +
                ",\"arg.properties\":{\"options\":[true]}},{\"type\":\"int\",\"arg.properties\":{\"options\":[42]}}," +
                "{\"type\":\"long\",\"arg.properties\":{\"options\":[4242424242424242]}},{\"type\":\"float\"," +
                "\"arg.properties\":{\"options\":[42.42]}},{\"type\":\"double\",\"arg.properties\"" +
                ":{\"options\":[4.242424242424242E7]}},{\"type\":\"bytes\",\"arg.properties\":{\"options\":[\"NDI=\"]}}," +
                "{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Forty-two\"]}}]}]}"

    private val avroSchema2 =
        "{\"type\":\"record\",\"name\":\"sampleRecordTwo\",\"namespace\":\"com.mycorp.mynamespace\",\"doc\":" +
                "\"Sample schema to help you get started.\",\"fields\":[{\"name\":\"my_field1\",\"type\":\"int\",\"doc\":" +
                "\"The int type is a 32-bit signed integer.\"},{\"name\":\"my_field2\",\"type\":\"double\",\"doc\":" +
                "\"The double type is a double precision (64-bit) IEEE 754 floating-point number.\",\"default\":10.5}," +
                "{\"name\":\"my_field3\",\"type\":\"string\",\"doc\":\"The string is a unicode character sequence.\"}]}"

    private val protobufSchema1 = "syntax = \"proto3\";\n" +
            "\n" +
            "message Person {\n" +
            "  string name = 1;\n" +
            "  int32 age = 2;\n" +
            "  Phone phones = 3;\n" +
            "  map<string, Email> emails = 4;\n" +
            "}\n" +
            "message Phone {\n" +
            "  string number = 1;\n" +
            "  PhoneType type = 2;\n" +
            "}\n" +
            "message Email {\n" +
            "  string address = 1;\n" +
            "  EmailType type = 2;\n" +
            "}\n" +
            "enum PhoneType {\n" +
            "  HOME = 0;\n" +
            "  WORK = 1;\n" +
            "  MOBILE = 2;\n" +
            "}\n" +
            "enum EmailType {\n" +
            "  PERSONAL = 0;\n" +
            "  WORK = 1;\n" +
            "}"

    private val protobufSchema2 = "syntax = \"proto3\";\n" +
            "\n" +
            "message Address {\n" +
            "  string street = 1;\n" +
            "  string city = 2;\n" +
            "  string state = 3;\n" +
            "}\n" +
            "\n" +
            "enum Status {\n" +
            "  UNKNOWN = 0;\n" +
            "  ACTIVE = 1;\n" +
            "  INACTIVE = 2;\n" +
            "}\n" +
            "\n" +
            "message Person {\n" +
            "  string name = 1;\n" +
            "  int32 age = 2;\n" +
            "  repeated string email = 3;\n" +
            "  repeated Address addresses = 4;\n" +
            "  map<string, int32> phone_numbers = 5;\n" +
            "  Status status = 6;\n" +
            "}\n" +
            "\n" +
            "message Organization {\n" +
            "  string name = 1;\n" +
            "  repeated Person employees = 2;\n" +
            "  map<int32, string> departments = 3;\n" +
            "}\n" +
            "\n" +
            "message MyMessage {\n" +
            "  string id = 1;\n" +
            "  Organization organization = 2;\n" +
            "}\n"

    private val jsonSchema1 = "{\n" +
            "  \"\$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"title\": \"Example Schema\",\n" +
            "  \"description\": \"This is an example JSON Schema\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"name\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 18,\n" +
            "      \"maximum\": 99\n" +
            "    },\n" +
            "    \"email\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"format\": \"email\"\n" +
            "    },\n" +
            "    \"isMarried\": {\n" +
            "      \"type\": \"boolean\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"name\",\n" +
            "    \"age\"\n" +
            "  ]\n" +
            "}"

    private val jsonSchema2 = "{\n" +
            "  \"\$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"title\": \"Example Schema\",\n" +
            "  \"description\": \"This is an example JSON Schema\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"name\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"minLength\": 1,\n" +
            "      \"maxLength\": 100\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 18,\n" +
            "      \"maximum\": 99\n" +
            "    },\n" +
            "    \"email\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"format\": \"email\"\n" +
            "    },\n" +
            "    \"isMarried\": {\n" +
            "      \"type\": \"boolean\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"name\",\n" +
            "    \"age\"\n" +
            "  ]\n" +
            "}"
}
