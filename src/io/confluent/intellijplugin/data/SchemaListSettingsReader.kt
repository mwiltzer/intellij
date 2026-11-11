package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.toolwindow.config.KafkaClusterConfig
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings

/** Makes the [SchemaListSettings] out of what the user chose in the tool window. */
internal object SchemaListSettingsReader {

    fun read(toolWindow: KafkaToolWindowSettings, config: KafkaClusterConfig): SchemaListSettings = SchemaListSettings(
        filter = config.schemaFilterName,
        pinned = config.schemasPined.toSet(),
        favoritesOnly = toolWindow.showFavoriteSchema,
        limit = config.registryLimit
    )
}
