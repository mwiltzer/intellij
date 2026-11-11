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
     * Whether the [name] passes the [filter]: it contains the filter, regardless of the case. No filter, or a blank
     * one, lets every name pass. The filter is plain text: characters such as `(` or `.` mean themselves.
     */
    fun matchesName(name: String, filter: String?): Boolean =
        filter.isNullOrBlank() || name.contains(filter, ignoreCase = true)

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
     *
     * [sortedByName] must be in the order of the names regardless of the case, which is the order of the result,
     * so a registry with thousands of schemas is not sorted again whenever the filter changes.
     */
    fun schemaListView(sortedByName: List<KafkaSchemaInfo>, settings: SchemaListSettings): SchemaListView {
        val limit = settings.limit
        val pinned = settings.pinned
        val favorites = ArrayList<KafkaSchemaInfo>()
        val others = ArrayList<KafkaSchemaInfo>()
        var matching = 0

        for (schema in sortedByName) {
            if (!matchesName(schema.name, settings.filter)) continue

            if (schema.name in pinned) {
                favorites += schema
                matching++
            } else if (!settings.favoritesOnly) {
                matching++
                // No more than can be shown are kept, and one more to tell that there are more.
                if (limit == null || others.size <= limit) others += schema
            }
        }

        val ordered = favorites + others
        val shown = if (limit != null) ordered.take(limit) else ordered
        return SchemaListView(
            schemas = shown.map { schema ->
                val isFavorite = schema.name in pinned
                if (schema.isFavorite == isFavorite) schema else schema.copy(isFavorite = isFavorite)
            },
            hasMore = limit != null && matching > limit
        )
    }
}

/** What shapes the schema list of the tool window, see [ClusterDataFilters.schemaListView]. */
internal data class SchemaListSettings(
    val filter: String?,
    val pinned: Set<String>,
    val favoritesOnly: Boolean,
    val limit: Int?
)

/** The schemas to show, and whether the limit left out more that match. */
internal data class SchemaListView(
    val schemas: List<KafkaSchemaInfo>,
    val hasMore: Boolean
)
