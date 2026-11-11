package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.toolwindow.config.KafkaClusterConfig
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings

/** Makes the [SchemaListSettings] out of what the user chose in the tool window. */
internal object SchemaListSettingsReader {

    /**
     * Whether the schemas of the topics that the user cannot see are left out. Only a Confluent registry has subjects,
     * which are named after topics, so the choice means nothing for any other.
     */
    fun hidesSchemasOfOtherTopics(registryType: KafkaRegistryType, toolWindow: KafkaToolWindowSettings): Boolean =
        registryType == KafkaRegistryType.CONFLUENT && !toolWindow.showOtherTopicsSchemas

    /**
     * @param visibleTopics the topics the user can see, as far as they are known. It is only asked for when the schemas
     * of other topics are hidden.
     */
    fun read(
        toolWindow: KafkaToolWindowSettings,
        config: KafkaClusterConfig,
        registryType: KafkaRegistryType,
        visibleTopics: () -> Set<String>?
    ): SchemaListSettings = SchemaListSettings(
        filter = config.schemaFilterName,
        pinned = config.schemasPined.toSet(),
        favoritesOnly = toolWindow.showFavoriteSchema,
        limit = config.registryLimit,
        showInternal = registryType != KafkaRegistryType.CONFLUENT || toolWindow.showInternalSchemas,
        visibleTopics = if (hidesSchemasOfOtherTopics(registryType, toolWindow)) visibleTopics() else null
    )
}
