package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.toolwindow.config.KafkaClusterConfig
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaListSettingsReaderTest {

    @Test
    fun `should take the filter, the limit and the favorites from the settings`() {
        val settings = SchemaListSettingsReader.read(
            toolWindow = KafkaToolWindowSettings().apply { showFavoriteSchema = true },
            config = KafkaClusterConfig(
                registryLimit = 25,
                schemaFilterName = "ord",
                schemasPined = mutableSetOf("a-value", "b-value")
            )
        )

        assertEquals("ord", settings.filter)
        assertEquals(25, settings.limit)
        assertEquals(setOf("a-value", "b-value"), settings.pinned)
        assertTrue(settings.favoritesOnly)
    }
}
