package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.toolwindow.config.KafkaClusterConfig
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaListSettingsReaderTest {

    private fun config(
        filter: String? = null,
        limit: Int? = 100,
        pinned: Set<String> = emptySet()
    ) = KafkaClusterConfig(registryLimit = limit, schemaFilterName = filter, schemasPined = pinned.toMutableSet())

    private fun toolWindow(
        favorites: Boolean = false,
        internal: Boolean = false,
        otherTopics: Boolean = false
    ) = KafkaToolWindowSettings().apply {
        showFavoriteSchema = favorites
        showInternalSchemas = internal
        showOtherTopicsSchemas = otherTopics
    }

    private fun read(
        toolWindow: KafkaToolWindowSettings = toolWindow(),
        config: KafkaClusterConfig = config(),
        registryType: KafkaRegistryType = KafkaRegistryType.CONFLUENT,
        visibleTopics: () -> Set<String>? = { setOf("orders") }
    ) = SchemaListSettingsReader.read(toolWindow, config, registryType, visibleTopics)

    @Test
    fun `should hide the internal schemas and those of other topics by default`() {
        assertFalse(KafkaToolWindowSettings().showInternalSchemas)
        assertFalse(KafkaToolWindowSettings().showOtherTopicsSchemas)

        val settings = read()

        assertFalse(settings.showInternal)
        assertEquals(setOf("orders"), settings.visibleTopics)
    }

    @Test
    fun `should take the filter, the limit and the favorites from the settings`() {
        val settings = read(
            toolWindow = toolWindow(favorites = true),
            config = config(filter = "ord", limit = 25, pinned = setOf("a-value", "b-value"))
        )

        assertEquals("ord", settings.filter)
        assertEquals(25, settings.limit)
        assertEquals(setOf("a-value", "b-value"), settings.pinned)
        assertTrue(settings.favoritesOnly)
    }

    @Test
    fun `should show the internal schemas when the user wants them`() {
        assertTrue(read(toolWindow = toolWindow(internal = true)).showInternal)
    }

    @Test
    fun `should not hide the schemas of other topics when the user wants them`() {
        val settings = read(toolWindow = toolWindow(otherTopics = true))

        assertNull(settings.visibleTopics)
    }

    @Test
    fun `should not ask for the topics when the schemas of other topics are shown`() {
        var asked = false

        read(toolWindow = toolWindow(otherTopics = true), visibleTopics = { asked = true; setOf("orders") })

        assertFalse(asked)
    }

    @Test
    fun `should hide nothing by topic while the topics are not known`() {
        val settings = read(visibleTopics = { null })

        assertNull(settings.visibleTopics)
        assertEquals(
            listOf("orders-value", "stranger-value"),
            ClusterDataFilters.schemaListView(listOf(KafkaSchemaInfo("orders-value"), KafkaSchemaInfo("stranger-value")), settings)
                .schemas.map { it.name }
        )
    }

    @Test
    fun `should leave a registry that is not a Confluent one alone`() {
        listOf(KafkaRegistryType.AWS_GLUE, KafkaRegistryType.NONE).forEach { type ->
            var asked = false

            val settings = read(registryType = type, visibleTopics = { asked = true; setOf("orders") })

            assertTrue(settings.showInternal, "$type")
            assertNull(settings.visibleTopics, "$type")
            assertFalse(asked, "$type")
            assertFalse(SchemaListSettingsReader.hidesSchemasOfOtherTopics(type, toolWindow()), "$type")
        }
    }

    @Test
    fun `should hide the schemas of other topics for a Confluent registry only`() {
        assertTrue(SchemaListSettingsReader.hidesSchemasOfOtherTopics(KafkaRegistryType.CONFLUENT, toolWindow()))
        assertFalse(SchemaListSettingsReader.hidesSchemasOfOtherTopics(KafkaRegistryType.CONFLUENT, toolWindow(otherTopics = true)))
    }
}
