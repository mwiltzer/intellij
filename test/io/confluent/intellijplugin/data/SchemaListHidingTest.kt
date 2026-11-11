package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.random.Random

class SchemaListHidingTest {

    @Nested
    inner class Subjects {

        @ParameterizedTest
        @CsvSource(
            "orders-value,orders-value",
            ":.ctx:orders-value,orders-value",
            ":.:orders-value,orders-value",
            ":.ctx.sub:orders-value,orders-value",
            ":.ctx:_internal-value,_internal-value",
            ":.broken,:.broken",
            "a:b,a:b",
            "ns:orders-value,ns:orders-value",
            "':.ctx:',''"
        )
        fun `should leave out the context a subject is qualified with`(subject: String, expected: String?) {
            assertEquals(expected ?: "", ClusterDataFilters.withoutContext(subject))
        }

        @ParameterizedTest
        @CsvSource(
            "_confluent-monitoring-value,true",
            "__consumer_offsets-value,true",
            "_x,true",
            ":.ctx:_x-value,true",
            ":.:_x-value,true",
            "orders-value,false",
            "orders_value,false",
            "ord_ers,false",
            ":.ctx:orders-value,false",
            ":.ctx_:orders-value,false",
            "x_:y,false"
        )
        fun `should take a subject for an internal one when its name starts with an underscore`(subject: String, internal: Boolean) {
            assertEquals(internal, ClusterDataFilters.isInternalSubject(subject))
        }

        @ParameterizedTest
        @CsvSource(
            "orders-value,orders",
            "orders-key,orders",
            "my-orders-value,my-orders",
            "my-orders-key,my-orders",
            "orders.v1-value,orders.v1",
            ":.ctx:orders-value,orders",
            ":.:orders-key,orders",
            "_confluent-monitoring-value,_confluent-monitoring",
            "orders-key-value,orders-key",
            "orders-value-key,orders-value"
        )
        fun `should tell the topic of a subject that is named the default way`(subject: String, topic: String) {
            assertEquals(topic, ClusterDataFilters.topicOfSubject(subject))
        }

        @ParameterizedTest
        @CsvSource(
            "com.example.Order",
            "com.example.Order-value.Record",
            "orders-com.example.Order",
            "orders",
            "value",
            "key",
            "-value",
            "-key",
            ":.ctx:-value",
            "orders-values",
            "orders-keys",
            "ordersvalue"
        )
        fun `should not tell a topic of a subject that is not named the default way`(subject: String) {
            assertNull(ClusterDataFilters.topicOfSubject(subject))
        }
    }

    private fun schemas(vararg names: String) = names.map { KafkaSchemaInfo(it) }

    private fun view(list: List<KafkaSchemaInfo>, settings: SchemaListSettings) =
        ClusterDataFilters.schemaListView(list.sortedBy { it.name.lowercase() }, settings)

    private fun settings(
        filter: String? = null,
        pinned: Set<String> = emptySet(),
        favoritesOnly: Boolean = false,
        limit: Int? = null,
        showInternal: Boolean = true,
        visibleTopics: Set<String>? = null
    ) = SchemaListSettings(filter, pinned, favoritesOnly, limit, showInternal, visibleTopics)

    private fun SchemaListView.names() = schemas.map { it.name }

    @Nested
    inner class Internal {

        private val registry = schemas("_confluent-monitoring-value", "__transaction-value", "orders-value", "orders-key", ":.ctx:_hidden-value")

        @Test
        fun `should leave out the internal schemas and count them`() {
            val view = view(registry, settings(showInternal = false))

            assertEquals(listOf("orders-key", "orders-value"), view.names())
            assertEquals(HiddenSchemas(internal = 3, otherTopics = 0), view.hidden)
        }

        @Test
        fun `should show everything when the internal schemas are to be shown`() {
            val view = view(registry, settings(showInternal = true))

            assertEquals(5, view.schemas.size)
            assertEquals(HiddenSchemas.NONE, view.hidden)
        }

        @Test
        fun `should show the internal schemas that are pinned`() {
            val view = view(registry, settings(showInternal = false, pinned = setOf("_confluent-monitoring-value")))

            assertEquals(listOf("_confluent-monitoring-value", "orders-key", "orders-value"), view.names())
            assertTrue(view.schemas.first().isFavorite)
            assertEquals(HiddenSchemas(internal = 2, otherTopics = 0), view.hidden)
        }

        @Test
        fun `should count only the internal schemas that match the filter`() {
            val view = view(registry, settings(showInternal = false, filter = "monitoring"))

            assertTrue(view.schemas.isEmpty())
            assertEquals(HiddenSchemas(internal = 1, otherTopics = 0), view.hidden)
        }

        @Test
        fun `should not count the hidden schemas for the limit`() {
            val list = schemas("_a-value", "_b-value", "_c-value", "x-value", "y-value")

            val view = view(list, settings(showInternal = false, limit = 2))

            assertEquals(listOf("x-value", "y-value"), view.names())
            assertFalse(view.hasMore)
            assertEquals(3, view.hidden.internal)
        }

        @Test
        fun `should say that there are more when the shown schemas are more than the limit`() {
            val list = schemas("_a-value", "x-value", "y-value", "z-value")

            val view = view(list, settings(showInternal = false, limit = 2))

            assertEquals(listOf("x-value", "y-value"), view.names())
            assertTrue(view.hasMore)
        }

        @Test
        fun `should hide nothing with favorites only, as it shows nothing but the pinned ones`() {
            val view = view(registry, settings(showInternal = false, favoritesOnly = true, pinned = setOf("orders-key")))

            assertEquals(listOf("orders-key"), view.names())
            assertEquals(HiddenSchemas.NONE, view.hidden)
        }
    }

    @Nested
    inner class OtherTopics {

        private val registry = schemas(
            "orders-value", "orders-key", "stranger-value", "stranger-key", "com.example.Order", "orders-com.example.Order",
            ":.ctx:orders-value", ":.ctx:stranger-value"
        )

        @Test
        fun `should leave out the schemas of the topics that are not visible`() {
            val view = view(registry, settings(visibleTopics = setOf("orders")))

            assertEquals(
                listOf(":.ctx:orders-value", "com.example.Order", "orders-com.example.Order", "orders-key", "orders-value"),
                view.names()
            )
            assertEquals(HiddenSchemas(internal = 0, otherTopics = 3), view.hidden)
        }

        @Test
        fun `should show the schemas that do not belong to one topic`() {
            val view = view(registry, settings(visibleTopics = setOf("something-else")))

            assertEquals(listOf("com.example.Order", "orders-com.example.Order"), view.names())
        }

        @Test
        fun `should show the schemas of the topics that are visible whichever the context`() {
            val view = view(registry, settings(visibleTopics = setOf("stranger")))

            assertTrue(":.ctx:stranger-value" in view.names())
            assertFalse(":.ctx:orders-value" in view.names())
        }

        @Test
        fun `should show everything when the topics are not known`() {
            val view = view(registry, settings(visibleTopics = null))

            assertEquals(registry.size, view.schemas.size)
            assertEquals(HiddenSchemas.NONE, view.hidden)
        }

        @Test
        fun `should show everything when no topic is visible`() {
            val view = view(registry, settings(visibleTopics = emptySet()))

            assertEquals(registry.size, view.schemas.size)
            assertEquals(HiddenSchemas.NONE, view.hidden)
        }

        @Test
        fun `should show the schemas of other topics that are pinned`() {
            val view = view(registry, settings(visibleTopics = setOf("orders"), pinned = setOf("stranger-value")))

            assertEquals("stranger-value", view.schemas.first().name)
            assertTrue(view.schemas.first().isFavorite)
            assertEquals(2, view.hidden.otherTopics)
        }

        @Test
        fun `should match the names of the topics as they are written`() {
            val view = view(schemas("Orders-value", "orders-value"), settings(visibleTopics = setOf("orders")))

            assertEquals(listOf("orders-value"), view.names())
            assertEquals(1, view.hidden.otherTopics)
        }

        @Test
        fun `should count only the schemas of other topics that match the filter`() {
            val view = view(registry, settings(visibleTopics = setOf("orders"), filter = "stranger"))

            assertTrue(view.schemas.isEmpty())
            assertEquals(3, view.hidden.otherTopics)
        }

        @Test
        fun `should not count the hidden schemas for the limit`() {
            val list = schemas("a-value", "b-value", "c-value", "x-value", "y-value")

            val view = view(list, settings(visibleTopics = setOf("x", "y"), limit = 2))

            assertEquals(listOf("x-value", "y-value"), view.names())
            assertFalse(view.hasMore)
            assertEquals(3, view.hidden.otherTopics)
        }
    }

    @Nested
    inner class Both {

        @Test
        fun `should count a schema once, as an internal one`() {
            val view = view(schemas("_x-value", "orders-value"), settings(showInternal = false, visibleTopics = setOf("orders")))

            assertEquals(listOf("orders-value"), view.names())
            assertEquals(HiddenSchemas(internal = 1, otherTopics = 0), view.hidden)
        }

        @Test
        fun `should still hide an internal schema of a topic that is not visible when internal ones are shown`() {
            val view = view(schemas("_x-value", "orders-value"), settings(showInternal = true, visibleTopics = setOf("orders")))

            assertEquals(listOf("orders-value"), view.names())
            assertEquals(HiddenSchemas(internal = 0, otherTopics = 1), view.hidden)
        }

        @Test
        fun `should add up what is hidden`() {
            assertEquals(5, HiddenSchemas(internal = 2, otherTopics = 3).total)
            assertEquals(0, HiddenSchemas.NONE.total)
        }

        @Test
        fun `should show the same as the reference, whatever the settings are`() {
            val random = Random(7)
            val parts = listOf("orders", "payments", "_internal", "x", "Orders", "a.b")
            val suffixes = listOf("-key", "-value", "-key", "-value", "-com.example.Record", "", "-values")
            val contexts = listOf("", "", "", ":.ctx:", ":.:", ":.a.b:")
            val names = (1..400).map {
                contexts.random(random) + parts.random(random) + (if (random.nextBoolean()) "-" + random.nextInt(4) else "") + suffixes.random(random)
            }.distinct()
            val listed = names.sortedBy { it.lowercase() }.map { KafkaSchemaInfo(it) }

            repeat(500) {
                val filter = listOf(null, "", "or", "_", "value", "ctx", "zzz").random(random)
                val pinned = names.filter { random.nextInt(12) == 0 }.toSet()
                val favoritesOnly = random.nextInt(5) == 0
                val limit = listOf(null, 0, 1, 5, 40, 1000).random(random)
                val showInternal = random.nextBoolean()
                val visibleTopics = listOf(null, emptySet(), setOf("orders"), setOf("orders", "payments", "x"), setOf("a.b")).random(random)
                val settings = settings(filter, pinned, favoritesOnly, limit, showInternal, visibleTopics)

                val expected = reference(listed, settings)
                val actual = ClusterDataFilters.schemaListView(listed, settings)

                val message = "settings=$settings"
                assertEquals(expected.names, actual.schemas.map { it.name }, message)
                assertEquals(expected.hasMore, actual.hasMore, message)
                assertEquals(expected.hidden, actual.hidden, message)
            }
        }
    }

    @Nested
    inner class Emptiness {

        @Test
        fun `should take a registry for empty when nothing is shown and nothing is hidden`() {
            assertTrue(ClusterDataFilters.isRegistryEmpty(0, null, false, HiddenSchemas.NONE))
            assertTrue(ClusterDataFilters.isRegistryEmpty(0, "  ", false, HiddenSchemas.NONE))
        }

        @Test
        fun `should not take a registry for empty when it shows schemas`() {
            assertFalse(ClusterDataFilters.isRegistryEmpty(1, null, false, HiddenSchemas.NONE))
        }

        @Test
        fun `should not take a registry for empty when a filter may keep its schemas from showing`() {
            assertFalse(ClusterDataFilters.isRegistryEmpty(0, "orders", false, HiddenSchemas.NONE))
            assertFalse(ClusterDataFilters.isRegistryEmpty(0, null, true, HiddenSchemas.NONE))
        }

        @Test
        fun `should not take a registry for empty when its schemas are hidden`() {
            assertFalse(ClusterDataFilters.isRegistryEmpty(0, null, false, HiddenSchemas(internal = 1, otherTopics = 0)))
            assertFalse(ClusterDataFilters.isRegistryEmpty(0, null, false, HiddenSchemas(internal = 0, otherTopics = 4)))
        }
    }

    private class Reference(val names: List<String>, val hasMore: Boolean, val hidden: HiddenSchemas)

    /** What the view should show, worked out the long way with regular expressions, one step after the other. */
    private fun reference(sortedByName: List<KafkaSchemaInfo>, settings: SchemaListSettings): Reference {
        val withoutContext = Regex("^(?::\\.[^:]*:)?(.*)$")
        val topicOf = Regex("^(.+)-(?:key|value)$")
        val topics = settings.visibleTopics?.takeIf { it.isNotEmpty() }

        fun internal(name: String) = !settings.showInternal && withoutContext.matchEntire(name)!!.groupValues[1].startsWith("_")
        fun ofOtherTopic(name: String): Boolean {
            if (topics == null) return false
            val topic = topicOf.matchEntire(withoutContext.matchEntire(name)!!.groupValues[1])?.groupValues?.get(1) ?: return false
            return topic !in topics
        }

        val matching = sortedByName.map { it.name }
            .filter { settings.filter.isNullOrBlank() || it.contains(settings.filter, ignoreCase = true) }
        val pinned = matching.filter { it in settings.pinned }
        val others = if (settings.favoritesOnly) emptyList() else matching.filter { it !in settings.pinned }

        val hiddenInternal = others.filter { internal(it) }
        val hiddenOther = others.filter { !internal(it) && ofOtherTopic(it) }
        val shown = pinned + others.filter { !internal(it) && !ofOtherTopic(it) }

        val limit = settings.limit
        return Reference(
            names = if (limit != null) shown.take(limit) else shown,
            hasMore = limit != null && shown.size > limit,
            hidden = HiddenSchemas(hiddenInternal.size, hiddenOther.size)
        )
    }
}
