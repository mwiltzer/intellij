package io.confluent.intellijplugin.common.editor

import io.confluent.intellijplugin.data.ClusterDataFilters

/**
 * The order of the schemas in the combo boxes of the consumer and producer editors.
 *
 * A registry can hold thousands of subjects, and the one the user wants is most likely one of a few. So these come
 * first, and nothing is left out:
 *  1. the schema of the topic by the naming convention of the registry, `<topic>-key` or `<topic>-value` for the
 *     combo box of the key or of the value,
 *  2. the subject that is named like the topic, which is taken for the value,
 *  3. the other schema of the topic, the one for the value in the combo box of the key and the other way round,
 *  4. any other schema that is named after the topic, such as `<topic>-value-v2`,
 *  5. the schemas the user pinned as favorites,
 *  6. all the others,
 *  7. the internal ones, that apps keep under a name that starts with an underscore.
 *
 * Within each of them the order is alphabetical, regardless of the case.
 */
internal object SchemaOrder {

    fun comparator(topic: String?, isKey: Boolean, pinned: Collection<String> = emptyList()): Comparator<String> {
        val rank = rankOf(topic.orEmpty(), isKey, pinned.toSet())
        // The case is only a tiebreaker, so that names that differ only by it do not change places between listings.
        return compareBy<String>({ rank(it) }, { it.lowercase() }, { it })
    }

    private fun rankOf(topic: String, isKey: Boolean, pinned: Set<String>): (String) -> Int {
        val own = if (isKey) "$topic$KEY_SUFFIX" else "$topic$VALUE_SUFFIX"
        val other = if (isKey) "$topic$VALUE_SUFFIX" else "$topic$KEY_SUFFIX"
        val hasTopic = topic.isNotEmpty()

        return { name ->
            when {
                hasTopic && name == own -> OWN
                hasTopic && !isKey && name == topic -> NAMED_LIKE_TOPIC
                hasTopic && name == other -> OTHER_OF_TOPIC
                hasTopic && ClusterDataFilters.withoutContext(name).startsWith("$topic-") -> NAMED_AFTER_TOPIC
                name in pinned -> PINNED
                ClusterDataFilters.isInternalSubject(name) -> INTERNAL
                else -> OTHERS
            }
        }
    }

    private const val KEY_SUFFIX = "-key"
    private const val VALUE_SUFFIX = "-value"

    private const val OWN = 0
    private const val NAMED_LIKE_TOPIC = 1
    private const val OTHER_OF_TOPIC = 2
    private const val NAMED_AFTER_TOPIC = 3
    private const val PINNED = 4
    private const val OTHERS = 5
    private const val INTERNAL = 6
}
