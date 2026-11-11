package io.confluent.intellijplugin.consumer.editor

import io.confluent.intellijplugin.common.editor.KafkaEditorUtils
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.consumer.models.ConsumerProducerFieldConfig
import io.confluent.intellijplugin.core.settings.connections.Property
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.nio.charset.StandardCharsets

/**
 * Holds only the rendered key/value text, never the deserialized objects: an Avro/Protobuf object graph
 * takes roughly five times the memory of its text, and the editors retain up to 50,000 records.
 */
data class KafkaRecord(
    val keyType: KafkaFieldType,
    val valueType: KafkaFieldType,
    val error: Throwable?,
    val keyText: String?,
    val valueText: String?,
    val topic: String,
    val partition: Int,
    val offset: Long,
    val duration: Long,
    val timestamp: Long,
    val keySize: Int,
    val valueSize: Int,
    val headers: List<Property>,
    val keyFormat: KafkaRegistryFormat,
    val valueFormat: KafkaRegistryFormat
) {
    val errorText = error?.message ?: error?.let { it::class.java.simpleName } ?: "<Unknown>"

    companion object {
        fun textOf(field: ConsumerProducerFieldConfig): String =
            KafkaEditorUtils.getValueAsString(field.type, field.getValueObj(), field.schemaFormat)

        fun createFor(
            keyType: KafkaFieldType?, valueType: KafkaFieldType?,
            keyFormat: KafkaRegistryFormat?, valueFormat: KafkaRegistryFormat?,
            record: Result<ConsumerRecord<Any, Any>>,
            errorPartition: Int? = null,
            errorOffset: Long? = null
        ) =
            if (record.isSuccess) {
                val rec = record.getOrNull()!!
                val resolvedKeyType = keyType ?: KafkaFieldType.STRING
                val resolvedValueType = valueType ?: KafkaFieldType.STRING
                val resolvedKeyFormat = keyFormat ?: KafkaRegistryFormat.UNKNOWN
                val resolvedValueFormat = valueFormat ?: KafkaRegistryFormat.UNKNOWN
                KafkaRecord(
                    keyType = resolvedKeyType,
                    valueType = resolvedValueType,
                    error = null,
                    keyText = KafkaEditorUtils.getValueAsString(resolvedKeyType, rec.key(), resolvedKeyFormat),
                    valueText = KafkaEditorUtils.getValueAsString(resolvedValueType, rec.value(), resolvedValueFormat),
                    topic = rec.topic(),
                    partition = rec.partition(),
                    offset = rec.offset(),
                    duration = -1,
                    timestamp = rec.timestamp(),
                    keySize = rec.serializedKeySize(),
                    valueSize = rec.serializedValueSize(),
                    headers = rec.headers()?.toList()?.map {
                        Property(
                            name = it.key() ?: "",
                            value = String(it.value() ?: byteArrayOf(0), StandardCharsets.UTF_8)
                        )
                    } ?: emptyList(),
                    keyFormat = resolvedKeyFormat,
                    valueFormat = resolvedValueFormat)
            } else {
                KafkaRecord(
                    keyType = keyType ?: KafkaFieldType.STRING,
                    valueType = valueType ?: KafkaFieldType.STRING,
                    error = record.exceptionOrNull(),
                    keyText = null,
                    valueText = null,
                    topic = "",
                    partition = errorPartition ?: -1,
                    offset = errorOffset ?: -1,
                    duration = -1,
                    timestamp = System.currentTimeMillis(),
                    keySize = 0,
                    valueSize = 0,
                    headers = emptyList(),
                    keyFormat = keyFormat ?: KafkaRegistryFormat.UNKNOWN,
                    valueFormat = valueFormat ?: KafkaRegistryFormat.UNKNOWN
                )
            }

        fun createFor(
            keyConfig: ConsumerProducerFieldConfig,
            valueConfig: ConsumerProducerFieldConfig,
            metadata: RecordMetadata?,
            duration: Long,
            headers: List<Property>
        ) = KafkaRecord(
            keyType = keyConfig.type,
            valueType = valueConfig.type,
            error = null,
            keyText = textOf(keyConfig),
            valueText = textOf(valueConfig),
            topic = metadata?.topic() ?: "",
            partition = metadata?.partition() ?: -1,
            offset = metadata?.offset() ?: -1,
            duration = duration,
            timestamp = metadata?.timestamp() ?: -1,
            keySize = metadata?.serializedKeySize() ?: 0,
            valueSize = metadata?.serializedValueSize() ?: 0,
            headers = headers.toList(),
            keyFormat = keyConfig.schemaFormat,
            valueFormat = valueConfig.schemaFormat
        )
    }
}