package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random

class SchemaListViewTest {

    private fun schemas(vararg names: String) = names.map { KafkaSchemaInfo(it) }

    private fun settings(
        filter: String? = null,
        pinned: Set<String> = emptySet(),
        favoritesOnly: Boolean = false,
        limit: Int? = null
    ) = SchemaListSettings(filter, pinned, favoritesOnly, limit)

    private fun view(list: List<KafkaSchemaInfo>, settings: SchemaListSettings) =
        ClusterDataFilters.schemaListView(list, settings)

    private val registry = schemas("alpha-key", "Alpha-value", "beta-value", "Gamma-key", "gamma-value", "orders(v1)-value")

    @Test
    fun `should show all schemas in the given order without a filter`() {
        val view = view(registry, settings())

        assertEquals(registry.map { it.name }, view.schemas.map { it.name })
        assertFalse(view.hasMore)
    }

    @Test
    fun `should keep the schemas whose name contains the filter regardless of the case`() {
        val view = view(registry, settings(filter = "GAMMA"))

        assertEquals(listOf("Gamma-key", "gamma-value"), view.schemas.map { it.name })
    }

    @Test
    fun `should take the filter literally`() {
        assertEquals(listOf("orders(v1)-value"), view(registry, settings(filter = "orders(v1")).schemas.map { it.name })
        assertTrue(view(registry, settings(filter = "g.mma")).schemas.isEmpty())
    }

    @Test
    fun `should ignore a blank filter`() {
        assertEquals(registry.size, view(registry, settings(filter = "   ")).schemas.size)
    }

    @Test
    fun `should show the pinned schemas first, in the order of the names, and mark them`() {
        val view = view(registry, settings(pinned = setOf("gamma-value", "beta-value")))

        assertEquals(
            listOf("beta-value", "gamma-value", "alpha-key", "Alpha-value", "Gamma-key", "orders(v1)-value"),
            view.schemas.map { it.name }
        )
        assertEquals(listOf(true, true, false, false, false, false), view.schemas.map { it.isFavorite })
    }

    @Test
    fun `should show only the pinned schemas with favorites only`() {
        val view = view(registry, settings(pinned = setOf("gamma-value", "beta-value"), favoritesOnly = true))

        assertEquals(listOf("beta-value", "gamma-value"), view.schemas.map { it.name })
        assertTrue(view.schemas.all { it.isFavorite })
    }

    @Test
    fun `should show nothing with favorites only when nothing is pinned`() {
        val view = view(registry, settings(favoritesOnly = true))

        assertTrue(view.schemas.isEmpty())
        assertFalse(view.hasMore)
    }

    @Test
    fun `should apply the filter to the pinned schemas, too`() {
        val view = view(registry, settings(filter = "alpha", pinned = setOf("gamma-value")))

        assertEquals(listOf("alpha-key", "Alpha-value"), view.schemas.map { it.name })
    }

    @Test
    fun `should ignore pinned schemas that the registry does not list`() {
        val view = view(registry, settings(pinned = setOf("deleted-value")))

        assertEquals(registry.map { it.name }, view.schemas.map { it.name })
        assertTrue(view.schemas.none { it.isFavorite })
    }

    @Test
    fun `should show no more than the limit and say that there are more`() {
        val view = view(registry, settings(limit = 4))

        assertEquals(registry.map { it.name }.take(4), view.schemas.map { it.name })
        assertTrue(view.hasMore)
    }

    @Test
    fun `should not say that there are more when exactly the limit matches`() {
        val view = view(registry, settings(limit = registry.size))

        assertEquals(registry.size, view.schemas.size)
        assertFalse(view.hasMore)
    }

    @Test
    fun `should show everything without a limit`() {
        val many = (1..500).map { KafkaSchemaInfo("schema-%04d".format(it)) }

        val view = view(many, settings(limit = null))

        assertEquals(500, view.schemas.size)
        assertFalse(view.hasMore)
    }

    @Test
    fun `should count only what matches the filter for the limit`() {
        val view = view(registry, settings(filter = "value", limit = 2))

        assertEquals(listOf("Alpha-value", "beta-value"), view.schemas.map { it.name })
        assertTrue(view.hasMore)
        assertFalse(view(registry, settings(filter = "orders", limit = 2)).hasMore)
    }

    @Test
    fun `should fill the limit with the pinned schemas first`() {
        val view = view(registry, settings(pinned = setOf("orders(v1)-value"), limit = 2))

        assertEquals(listOf("orders(v1)-value", "alpha-key"), view.schemas.map { it.name })
        assertTrue(view.hasMore)
    }

    @Test
    fun `should count only the pinned schemas for the limit with favorites only`() {
        val view = view(registry, settings(pinned = setOf("alpha-key", "beta-value", "gamma-value"), favoritesOnly = true, limit = 2))

        assertEquals(listOf("alpha-key", "beta-value"), view.schemas.map { it.name })
        assertTrue(view.hasMore)
    }

    @Test
    fun `should show nothing for a limit of zero, but say that there are more`() {
        val view = view(registry, settings(limit = 0))

        assertTrue(view.schemas.isEmpty())
        assertTrue(view.hasMore)
    }

    @Test
    fun `should not copy a schema whose favorite mark is right already`() {
        val pinned = KafkaSchemaInfo("alpha", isFavorite = true)
        val other = KafkaSchemaInfo("beta")

        val view = view(listOf(pinned, other), settings(pinned = setOf("alpha")))

        assertSame(pinned, view.schemas[0])
        assertSame(other, view.schemas[1])
    }

    @Test
    fun `should take a favorite mark back that does not hold any longer`() {
        val view = view(listOf(KafkaSchemaInfo("alpha", isFavorite = true)), settings())

        assertFalse(view.schemas.single().isFavorite)
    }

    @Test
    fun `should leave the details of a schema as they are`() {
        val detailed = KafkaSchemaInfo("alpha", type = KafkaRegistryFormat.AVRO, version = 7, compatibility = "FULL")

        val shown = view(listOf(detailed), settings(pinned = setOf("alpha"))).schemas.single()

        assertEquals(detailed.copy(isFavorite = true), shown)
    }

    @Test
    fun `should show the same as filtering, sorting and limiting one after the other`() {
        val random = Random(42)
        val alphabet = "abcAB-_.("
        val names = (1..300).map { index ->
            val name = buildString { repeat(random.nextInt(1, 6)) { append(alphabet[random.nextInt(alphabet.length)]) } }
            "$name-$index"
        }.sortedBy { it.lowercase() }
        val listed = names.map { KafkaSchemaInfo(it) }

        repeat(400) {
            val filter = listOf(null, "", "a", "B", "a-", "(", ".b", "zzz", "-1").random(random)
            val pinned = names.filter { random.nextInt(10) == 0 }.toSet()
            val favoritesOnly = random.nextBoolean()
            val limit = listOf(null, 0, 1, 5, 50, 100, 1000).random(random)

            val expected = ClusterDataFilters
                .sortSchemasWithFavorites(ClusterDataFilters.applySchemaFilters(listed, filter), pinned, favoritesOnly)
                .let { sorted ->
                    if (limit != null && sorted.size > limit) sorted.take(limit) to true else sorted to false
                }

            val actual = view(listed, settings(filter, pinned, favoritesOnly, limit))

            assertEquals(expected.first, actual.schemas, "filter=$filter favoritesOnly=$favoritesOnly limit=$limit")
            assertEquals(expected.second, actual.hasMore, "filter=$filter favoritesOnly=$favoritesOnly limit=$limit")
        }
    }
}
