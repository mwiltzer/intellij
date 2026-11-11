package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.model.ConsumerGroupPresentable
import io.confluent.intellijplugin.model.TopicPresentable
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo

/**
 * Pure utility functions for filtering, sorting, and paginating cluster data.
 * Used exclusively in [BaseClusterDataManager].
 */
internal object ClusterDataFilters {

    fun applyTopicFilters(
        topics: List<TopicPresentable>,
        showInternalTopics: Boolean,
        filterName: String?
    ): List<TopicPresentable> {
        return topics.filter { topic ->
            (showInternalTopics || !topic.internal) &&
                (filterName == null || topic.name.contains(filterName, ignoreCase = true))
        }
    }

    fun sortTopicsWithFavorites(
        topics: List<TopicPresentable>,
        pinnedTopics: Set<String>,
        showFavoriteOnly: Boolean
    ): List<TopicPresentable> {
        val topicsWithFavorites = topics.map { topic ->
            topic.copy(isFavorite = pinnedTopics.contains(topic.name))
        }

        val filteredTopics = if (showFavoriteOnly) {
            topicsWithFavorites.filter { it.isFavorite }
        } else {
            topicsWithFavorites
        }

        return filteredTopics.sortedWith(
            compareByDescending<TopicPresentable> { it.isFavorite }
                .thenBy { it.name.lowercase() }
        )
    }

    fun applyTopicLimit(
        topics: List<TopicPresentable>,
        limit: Int?
    ): Pair<List<TopicPresentable>, Boolean> {
        return if (limit != null && topics.size > limit) {
            topics.take(limit) to true
        } else {
            topics to false
        }
    }

    fun applyConsumerGroupFilters(
        groups: List<ConsumerGroupPresentable>,
        filterName: String?
    ): List<ConsumerGroupPresentable> {
        return groups.filter { group ->
            filterName == null || group.consumerGroup.contains(filterName, ignoreCase = true)
        }
    }

    fun applyConsumerGroupLimit(
        groups: List<ConsumerGroupPresentable>,
        limit: Int?
    ): Pair<List<ConsumerGroupPresentable>, Boolean> {
        return if (limit != null && groups.size > limit) {
            groups.take(limit) to true
        } else {
            groups to false
        }
    }

    fun applySchemaFilters(
        schemas: List<KafkaSchemaInfo>,
        filterName: String?
    ): List<KafkaSchemaInfo> {
        return schemas.filter { schema -> matchesName(schema.name, filterName) }
    }

    /**
     * Whether the [name] passes the [filter]. No filter, or a blank one, lets every name pass.
     *
     * A filter that starts with `^` asks for the names that start with what follows it, see [prefixOf], and takes the
     * case as it is. Any other filter is a text the name contains, regardless of the case. The filter is plain
     * text: characters such as `(` or `.` mean themselves.
     */
    fun matchesName(name: String, filter: String?): Boolean {
        if (filter.isNullOrBlank()) return true
        val prefix = prefixOf(filter) ?: return name.contains(filter, ignoreCase = true)
        return name.startsWith(prefix)
    }

    /**
     * The prefix that the [filter] asks the names to start with: `orders` for `^orders`. Empty for a `^` alone, that is
     * a filter that is still being typed and lets every name pass. `null` for a filter that is no prefix.
     *
     * The registry can list the subjects with a prefix, which is much cheaper than listing all of them.
     */
    fun prefixOf(filter: String?): String? =
        filter?.takeIf { it.startsWith(PREFIX_MARK) }?.substring(PREFIX_MARK.length)

    private const val PREFIX_MARK = "^"

    fun sortSchemasWithFavorites(
        schemas: List<KafkaSchemaInfo>,
        pinnedSchemas: Set<String>,
        showFavoriteOnly: Boolean
    ): List<KafkaSchemaInfo> {
        val schemasWithFavorites = schemas.map { schema ->
            schema.copy(isFavorite = pinnedSchemas.contains(schema.name))
        }

        val filteredSchemas = if (showFavoriteOnly) {
            schemasWithFavorites.filter { it.isFavorite }
        } else {
            schemasWithFavorites
        }

        return filteredSchemas.sortedWith(
            compareByDescending<KafkaSchemaInfo> { it.isFavorite }
                .thenBy { it.name.lowercase() }
        )
    }

    /**
     * The schemas the tool window shows out of everything the registry lists: those that match the filter, the
     * pinned ones first (only those with [SchemaListSettings.favoritesOnly]), then no more than the limit.
     * Schemas that the settings hide are left out and counted, but never the pinned ones.
     *
     * [sortedByName] must be in the order of the names regardless of the case, which is the order of the result,
     * so a registry with thousands of schemas is not sorted again whenever the filter changes.
     */
    fun schemaListView(sortedByName: List<KafkaSchemaInfo>, settings: SchemaListSettings): SchemaListView {
        val limit = settings.limit
        val pinned = settings.pinned
        val topics = settings.visibleTopics?.takeIf { it.isNotEmpty() }
        val favorites = ArrayList<KafkaSchemaInfo>()
        val others = ArrayList<KafkaSchemaInfo>()
        var matching = 0
        var hiddenInternal = 0
        var hiddenOtherTopics = 0

        for (schema in sortedByName) {
            if (!matchesName(schema.name, settings.filter)) continue

            if (schema.name in pinned) {
                favorites += schema
                matching++
            } else if (!settings.favoritesOnly) {
                when (hiddenBy(schema.name, settings.showInternal, topics)) {
                    HiddenBy.INTERNAL -> hiddenInternal++
                    HiddenBy.OTHER_TOPIC -> hiddenOtherTopics++
                    null -> {
                        matching++
                        // No more than can be shown are kept, and one more to tell that there are more.
                        if (limit == null || others.size <= limit) others += schema
                    }
                }
            }
        }

        val ordered = favorites + others
        val shown = if (limit != null) ordered.take(limit) else ordered
        return SchemaListView(
            schemas = shown.map { schema ->
                val isFavorite = schema.name in pinned
                if (schema.isFavorite == isFavorite) schema else schema.copy(isFavorite = isFavorite)
            },
            hasMore = limit != null && matching > limit,
            hidden = HiddenSchemas(internal = hiddenInternal, otherTopics = hiddenOtherTopics)
        )
    }

    /**
     * The name of a schema subject without the context it is qualified with: `:.ctx:orders-value` is `orders-value`.
     */
    fun withoutContext(subject: String): String {
        if (!subject.startsWith(CONTEXT_PREFIX)) return subject
        val end = subject.indexOf(':', startIndex = CONTEXT_PREFIX.length)
        return if (end < 0) subject else subject.substring(end + 1)
    }

    /** Whether the subject is for something internal: apps keep their subjects under a name that starts with `_`. */
    fun isInternalSubject(subject: String): Boolean = withoutContext(subject).startsWith('_')

    /**
     * The topic a subject is for when it is named the default way, `<topic>-key` or `<topic>-value`. `null` for
     * the others: a name after the record, such as `com.example.Order`, does not say which topic it is for.
     */
    fun topicOfSubject(subject: String): String? {
        val name = withoutContext(subject)
        val topic = when {
            name.endsWith(VALUE_SUFFIX) -> name.removeSuffix(VALUE_SUFFIX)
            name.endsWith(KEY_SUFFIX) -> name.removeSuffix(KEY_SUFFIX)
            else -> return null
        }
        return topic.ifEmpty { null }
    }

    /**
     * Why the subject [name] is not shown, or `null` if it is. A subject is only taken for one of another topic
     * if it is named after a topic, and the topics that are visible are known.
     */
    private fun hiddenBy(name: String, showInternal: Boolean, visibleTopics: Set<String>?): HiddenBy? {
        if (!showInternal && isInternalSubject(name)) return HiddenBy.INTERNAL
        if (visibleTopics == null) return null
        val topic = topicOfSubject(name) ?: return null
        return if (topic in visibleTopics) null else HiddenBy.OTHER_TOPIC
    }

    /**
     * Whether the registry has no schemas as far as anyone can tell, as opposed to a filter or a setting hiding the
     * ones it has. Only then is the user asked to create a schema.
     */
    fun isRegistryEmpty(shown: Int, filter: String?, favoritesOnly: Boolean, hidden: HiddenSchemas): Boolean =
        shown == 0 && filter.isNullOrBlank() && !favoritesOnly && hidden.total == 0

    private enum class HiddenBy { INTERNAL, OTHER_TOPIC }

    private const val CONTEXT_PREFIX = ":."
    private const val VALUE_SUFFIX = "-value"
    private const val KEY_SUFFIX = "-key"
}

/** What shapes the schema list of the tool window, see [ClusterDataFilters.schemaListView]. */
internal data class SchemaListSettings(
    val filter: String?,
    val pinned: Set<String>,
    val favoritesOnly: Boolean,
    val limit: Int?,
    /** `false` hides the schemas of internal subjects, see [ClusterDataFilters.isInternalSubject]. */
    val showInternal: Boolean = true,
    /**
     * The topics that the user can see. The schemas of other topics are hidden, see [ClusterDataFilters.topicOfSubject].
     * `null` or empty when they are not known, so that nothing is hidden for a reason nobody can check.
     */
    val visibleTopics: Set<String>? = null
)

/** The schemas to show, whether the limit left out more that match, and what the settings left out. */
internal data class SchemaListView(
    val schemas: List<KafkaSchemaInfo>,
    val hasMore: Boolean,
    val hidden: HiddenSchemas = HiddenSchemas.NONE
)

/** How many of the schemas that match the filter the settings hide, by what the settings hide them. */
internal data class HiddenSchemas(val internal: Int, val otherTopics: Int) {
    val total: Int get() = internal + otherTopics

    companion object {
        val NONE = HiddenSchemas(internal = 0, otherTopics = 0)
    }
}
