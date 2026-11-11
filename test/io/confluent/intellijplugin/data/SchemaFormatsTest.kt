package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class SchemaFormatsTest {

    /** A registry that lists all schemas and, if told to, those of a topic faster. */
    private class Source(
        private val all: List<String>?,
        private val ofTopic: Map<String, List<String>>? = null
    ) : SchemaFormats {
        var allRequests = 0
        val topicRequests = mutableListOf<String>()

        override suspend fun getSchemaNamesForEditor(): List<String>? {
            allRequests++
            return all
        }

        override suspend fun getSchemaNamesForTopic(topic: String): List<String>? {
            if (ofTopic == null) return super.getSchemaNamesForTopic(topic)
            topicRequests += topic
            return ofTopic[topic]
        }

        override fun getKnownSchemaFormat(name: String): KafkaRegistryFormat? = null
        override suspend fun getSchemaFormat(name: String): KafkaRegistryFormat? = null
        override suspend fun resolveSchemaFormats(
            names: Collection<String>,
            first: Collection<String>,
            onResolved: (name: String, format: KafkaRegistryFormat) -> Unit
        ) = Unit
    }

    @Test
    fun `should have no faster way to the schemas of a topic unless the registry has one`() = runBlocking<Unit> {
        val source = Source(all = listOf("orders-value"))

        assertNull(source.getSchemaNamesForTopic("orders"))
        assertEquals(0, source.allRequests)
    }

    @Test
    fun `should take the names of the topic when the registry tells them`() = runBlocking<Unit> {
        val source = Source(all = listOf("audit-value", "orders-value"), ofTopic = mapOf("orders" to listOf("orders-value")))

        assertEquals(listOf("orders-value"), source.getSchemaNamesToFindTopicSchemas("orders"))

        assertEquals(listOf("orders"), source.topicRequests)
        assertEquals(0, source.allRequests)
    }

    @Test
    fun `should take all the names when the registry has no faster way`() = runBlocking<Unit> {
        val source = Source(all = listOf("audit-value", "orders-value"))

        assertEquals(listOf("audit-value", "orders-value"), source.getSchemaNamesToFindTopicSchemas("orders"))
    }

    @Test
    fun `should take all the names when the registry could not tell those of the topic`() = runBlocking<Unit> {
        val source = Source(all = listOf("audit-value", "orders-value"), ofTopic = emptyMap())

        assertEquals(listOf("audit-value", "orders-value"), source.getSchemaNamesToFindTopicSchemas("orders"))

        assertEquals(listOf("orders"), source.topicRequests)
        assertEquals(1, source.allRequests)
    }

    @Test
    fun `should give no names when the registry could not be asked at all`() = runBlocking<Unit> {
        val source = Source(all = null)

        assertEquals(emptyList<String>(), source.getSchemaNamesToFindTopicSchemas("orders"))
    }
}
