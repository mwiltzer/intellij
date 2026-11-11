package io.confluent.intellijplugin.consumer.models

import io.confluent.intellijplugin.registry.SchemaIdHeaders
import org.apache.kafka.clients.consumer.ConsumerRecord

data class ConsumerFilter(
    val filterKey: String?,
    val filterValue: String?,
    val filterHeadKey: String?,
    val filterHeadValue: String?,
    val type: ConsumerFilterType
) {

    fun isRecordPassFilter(record: ConsumerRecord<Any, Any>): Boolean =
        isPassFilter(record.key()?.toString(), filterKey) &&
                isPassFilter(record.value()?.toString(), filterValue) &&
                isPassFilterHeaders(record)

    private fun isPassFilterHeaders(record: ConsumerRecord<Any, Any>): Boolean {
        // What the plugin noted in the headers of the record is no part of what the producer sent.
        val headers = SchemaIdHeaders.visible(record.headers())
        return isPassFilterHeaders(headers.map { it.key() ?: "" }, filterHeadKey) &&
                isPassFilterHeaders(headers.mapNotNull { it.value()?.decodeToString() }, filterHeadValue)
    }

    private fun isPassFilter(value: String?, filterValue: String?): Boolean {
        if (filterValue == null)
            return true

        return when (type) {
            ConsumerFilterType.NONE -> true
            ConsumerFilterType.CONTAINS -> value?.contains(filterValue) == true
            ConsumerFilterType.DOES_NOT_CONTAINS -> value?.contains(filterValue) == false
            ConsumerFilterType.REGEX -> value?.matches(Regex(filterValue)) == true
        }
    }

    private fun isPassFilterHeaders(value: List<String>, filterValue: String?): Boolean {
        if (filterValue == null)
            return true

        return when (type) {
            ConsumerFilterType.NONE -> true
            ConsumerFilterType.CONTAINS -> value.any { it.contains(filterValue) }
            ConsumerFilterType.DOES_NOT_CONTAINS -> value.all { !it.contains(filterValue) }
            ConsumerFilterType.REGEX -> value.any {
                val regex = Regex(filterValue)
                regex.matches(it)
            }
        }
    }
}