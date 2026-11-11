package io.confluent.intellijplugin.common.models

import io.confluent.intellijplugin.registry.SchemaIdHeaders
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer

/**
 * Deserializes with the [delegate], and notes which schema the key or value was written with in the headers of the
 * record, so it can be shown with the record, see [SchemaIdHeaders]. The Kafka client hands the deserializers the
 * headers that the record is made with.
 */
internal class SchemaIdRecordingDeserializer<T>(
    private val delegate: Deserializer<T>,
    private var isKey: Boolean = false
) : Deserializer<T> {

    override fun configure(configs: Map<String, *>?, isKey: Boolean) {
        this.isKey = isKey
        delegate.configure(configs, isKey)
    }

    override fun deserialize(topic: String?, data: ByteArray?): T = delegate.deserialize(topic, data)

    override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): T {
        SchemaIdHeaders.record(headers, isKey, data)
        return delegate.deserialize(topic, headers, data)
    }

    override fun close() = delegate.close()
}
