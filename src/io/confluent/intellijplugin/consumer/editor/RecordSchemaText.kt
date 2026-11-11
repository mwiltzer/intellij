package io.confluent.intellijplugin.consumer.editor

import io.confluent.intellijplugin.registry.RecordSchema
import io.confluent.intellijplugin.registry.SchemaUsage
import io.confluent.intellijplugin.util.KafkaMessagesBundle

/** What the details of a record say about the schema it was written with. */
internal object RecordSchemaText {

    /** More subjects than that are counted and not listed: a schema can be registered under hundreds of them. */
    const val MAX_USAGES_SHOWN = 3

    private const val SEPARATOR = "; "

    /**
     * The ID of the schema, with the subjects and versions it is registered as when they are known; the GUID of a
     * schema that has no ID. Nothing ("") for a record that does not say. The subjects of the [topic] come first.
     */
    fun of(schema: RecordSchema?, usages: List<SchemaUsage>? = null, topic: String? = null): String {
        val id = schema?.id
        if (id != null) {
            // The ID is given as a string: a number would be formatted with the grouping of the locale.
            return withUsages(id.toString(), usages.orEmpty(), topic)
        }
        val guid = schema?.guid ?: return ""
        return KafkaMessagesBundle.message("consumer.record.schema.guid", guid.toString())
    }

    private fun withUsages(id: String, usages: List<SchemaUsage>, topic: String?): String {
        if (usages.isEmpty()) return id

        val ordered = usages.sortedWith(
            compareBy<SchemaUsage>({ !isSubjectOfTopic(it.subject, topic) }, { it.subject.lowercase() }, { it.version })
        )
        val listed = ordered.take(MAX_USAGES_SHOWN).map {
            KafkaMessagesBundle.message("consumer.record.schema.usage", it.subject, it.version.toString())
        }
        val more = ordered.size - listed.size
        val all = if (more > 0) listed + KafkaMessagesBundle.message("consumer.record.schema.usages.more", more.toString()) else listed
        return KafkaMessagesBundle.message("consumer.record.schema.with.usages", id, all.joinToString(SEPARATOR))
    }

    // The subjects of the topic by the naming convention of the registry.
    private fun isSubjectOfTopic(subject: String, topic: String?) =
        topic != null && (subject == topic || subject == "$topic-key" || subject == "$topic-value")
}
