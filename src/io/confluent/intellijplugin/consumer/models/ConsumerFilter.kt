package io.confluent.intellijplugin.consumer.models

import io.confluent.intellijplugin.util.KafkaMessagesBundle
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.PatternSyntaxException

data class ConsumerFilter(
    val filterKey: String?,
    val filterValue: String?,
    val filterHeadKey: String?,
    val filterHeadValue: String?,
    val type: ConsumerFilterType
) {

    // A filter is asked about every consumed record (and each of its headers), so a pattern is compiled once.
    private val patterns = ConcurrentHashMap<String, Regex>()

    internal fun pattern(source: String): Regex = patterns.getOrPut(source) { Regex(source) }

    /**
     * The check the consumers make: a key or value is matched on the text its field shows it with, which is what the
     * user sees in the table.
     */
    fun predicateFor(
        keyField: ConsumerProducerFieldConfig,
        valueField: ConsumerProducerFieldConfig
    ): (ConsumerRecord<Any, Any>) -> Boolean {
        val keyText = keyField::shownText
        val valueText = valueField::shownText
        return { record -> isRecordPassFilter(record, keyText, valueText) }
    }

    /**
     * Whether the [record] passes the filter.
     *
     * A key or value is matched on the text it is shown with ([keyText], [valueText]) and, so that the filters which
     * worked before still do, on its plain `toString()`. Making the shown text takes some work, so it is made only for
     * a key or value that has a filter, and only when the plain text did not decide it already.
     */
    fun isRecordPassFilter(
        record: ConsumerRecord<Any, Any>,
        keyText: (Any) -> String = Any::toString,
        valueText: (Any) -> String = Any::toString
    ): Boolean =
        isPassFilter(record.key(), filterKey, keyText) &&
                isPassFilter(record.value(), filterValue, valueText) &&
                (filterHeadKey == null && filterHeadValue == null || isPassFilterHeaders(record))

    private fun isPassFilterHeaders(record: ConsumerRecord<Any, Any>): Boolean {
        val headers = record.headers()?.toList() ?: emptyList()
        return isPassFilterHeaders(headers.map { it.key() ?: "" }, filterHeadKey) &&
                isPassFilterHeaders(headers.mapNotNull { it.value()?.decodeToString() }, filterHeadValue)
    }

    private fun isPassFilter(value: Any?, filterValue: String?, shownText: (Any) -> String): Boolean {
        if (filterValue == null)
            return true

        return when (type) {
            ConsumerFilterType.NONE -> true
            ConsumerFilterType.CONTAINS -> value != null &&
                    (value.toString().contains(filterValue) || shownText(value).contains(filterValue))

            ConsumerFilterType.DOES_NOT_CONTAINS -> value != null &&
                    !value.toString().contains(filterValue) && !shownText(value).contains(filterValue)

            ConsumerFilterType.REGEX -> value != null && pattern(filterValue).let {
                value.toString().matches(it) || shownText(value).matches(it)
            }
        }
    }

    private fun isPassFilterHeaders(value: List<String>, filterValue: String?): Boolean {
        if (filterValue == null)
            return true

        return when (type) {
            ConsumerFilterType.NONE -> true
            ConsumerFilterType.CONTAINS -> value.any { it.contains(filterValue) }
            ConsumerFilterType.DOES_NOT_CONTAINS -> value.all { !it.contains(filterValue) }
            ConsumerFilterType.REGEX -> value.any { pattern(filterValue).matches(it) }
        }
    }

    companion object {
        /**
         * What is wrong with [source] as a regular expression, worded for the user, or null when it is a valid one.
         * An empty text is no filter at all, so it is fine, too.
         */
        fun regexProblem(source: String): String? {
            if (source.isBlank())
                return null

            return try {
                Regex(source)
                null
            } catch (e: PatternSyntaxException) {
                describe(e)
            }
        }

        internal fun describe(e: PatternSyntaxException): String =
            if (e.index >= 0)
                KafkaMessagesBundle.message("consumer.filter.regex.invalid.at", e.description.orEmpty(), e.index + 1)
            else
                KafkaMessagesBundle.message("consumer.filter.regex.invalid", e.description.orEmpty())
    }
}