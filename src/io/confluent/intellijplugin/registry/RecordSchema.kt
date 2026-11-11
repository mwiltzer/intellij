package io.confluent.intellijplugin.registry

import io.confluent.kafka.serializers.schema.id.SchemaId
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import java.nio.ByteBuffer
import java.util.UUID

/**
 * The schema a key or a value says it was written with, as far as the record tells: the ID the registry gave the
 * schema (wire format V0), or the GUID of the schema (wire format V1). Only one of them is set.
 */
data class RecordSchema(val id: Int? = null, val guid: UUID? = null)

/** A subject of the registry, and the version of it that a schema is registered as. */
data class SchemaUsage(val subject: String, val version: Int)

/**
 * Hands the schema of a record from where it is deserialized to where it is shown.
 *
 * Keys and values are deserialized before anybody sees the record, and the deserializers do not tell which schema
 * they used. The schema ID of the wire format V0 is the first five bytes of the payload, so it is noted in a header
 * of the record, one that only the plugin knows about, while the payload is at hand. The GUID of the wire format V1
 * is a header of the producer's already.
 */
object SchemaIdHeaders {
    // Another name than the ones of the clients that write the schema into a header themselves.
    private const val PREFIX = "confluent.intellij."
    private const val KEY_ID_HEADER = PREFIX + "key.schemaId"
    private const val VALUE_ID_HEADER = PREFIX + "value.schemaId"

    private const val ID_SIZE = Int.SIZE_BYTES
    private const val GUID_SIZE = 2 * Long.SIZE_BYTES

    /** Whether the header is one that [record] wrote, and no part of what the producer sent. */
    fun isReserved(name: String?): Boolean = name?.startsWith(PREFIX) == true

    /**
     * Notes the schema ID that the [payload] of a key or value starts with. Does nothing for a payload that is not
     * written in the wire format V0, or for headers that can not be changed.
     */
    fun record(headers: Headers?, isKey: Boolean, payload: ByteArray?) {
        if (headers == null || payload == null || payload.size < 1 + ID_SIZE || payload[0] != SchemaId.MAGIC_BYTE_V0)
            return

        val name = if (isKey) KEY_ID_HEADER else VALUE_ID_HEADER
        try {
            headers.remove(name)
            headers.add(name, payload.copyOfRange(1, 1 + ID_SIZE))
        } catch (_: IllegalStateException) {
            // Headers that are read-only keep what they have. The schema is shown when it can be told, no more.
        }
    }

    /** What the headers of a record say about the schema of its key or value, or `null` if they say nothing. */
    fun schemaOf(headers: Headers?, isKey: Boolean): RecordSchema? {
        if (headers == null) return null

        val id = headers.lastHeader(if (isKey) KEY_ID_HEADER else VALUE_ID_HEADER)?.value()
        if (id != null && id.size == ID_SIZE) return RecordSchema(id = ByteBuffer.wrap(id).getInt())

        val guid = headers.lastHeader(if (isKey) SchemaId.KEY_SCHEMA_ID_HEADER else SchemaId.VALUE_SCHEMA_ID_HEADER)
            ?.value()
        if (guid != null && guid.size >= 1 + GUID_SIZE && guid[0] == SchemaId.MAGIC_BYTE_V1) {
            val buffer = ByteBuffer.wrap(guid, 1, GUID_SIZE)
            return RecordSchema(guid = UUID(buffer.getLong(), buffer.getLong()))
        }
        return null
    }

    /** The headers the producer of a record sent: all but the ones [record] wrote. */
    fun visible(headers: Headers?): List<Header> = headers?.filter { !isReserved(it.key()) } ?: emptyList()
}
