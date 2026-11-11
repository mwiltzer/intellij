package io.confluent.intellijplugin.common.editor

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SchemaOrderTest {

    private fun order(
        names: List<String>,
        topic: String? = "orders",
        isKey: Boolean = false,
        pinned: Collection<String> = emptyList()
    ) = names.sortedWith(SchemaOrder.comparator(topic, isKey, pinned))

    @Test
    fun `should list all schemas alphabetically regardless of the case when there is no topic`() {
        val names = listOf("b-value", "A-value", "c-value", "a-key")

        assertEquals(listOf("a-key", "A-value", "b-value", "c-value"), order(names, topic = null))
    }

    @Test
    fun `should treat an empty topic like no topic`() {
        val names = listOf("orders-value", "audit-value")

        assertEquals(listOf("audit-value", "orders-value"), order(names, topic = ""))
    }

    @Test
    fun `should list the value schema of the topic first for the value`() {
        val names = listOf("audit-value", "orders-value", "payments-value")

        assertEquals(listOf("orders-value", "audit-value", "payments-value"), order(names))
    }

    @Test
    fun `should list the key schema of the topic first for the key`() {
        val names = listOf("audit-key", "orders-key", "orders-value", "payments-key")

        assertEquals(listOf("orders-key", "orders-value", "audit-key", "payments-key"), order(names, isKey = true))
    }

    @Test
    fun `should list the value schema before the other one of the topic for the value`() {
        val names = listOf("orders-key", "orders-value")

        assertEquals(listOf("orders-value", "orders-key"), order(names))
    }

    @Test
    fun `should take the subject that is named like the topic for the value`() {
        val names = listOf("audit-value", "orders", "orders-key")

        assertEquals(listOf("orders", "orders-key", "audit-value"), order(names))
    }

    @Test
    fun `should not take the subject that is named like the topic for the key`() {
        val names = listOf("audit-value", "orders", "orders-key")

        assertEquals(listOf("orders-key", "audit-value", "orders"), order(names, isKey = true))
    }

    @Test
    fun `should list the schema that suits the topic before the one that is named after it`() {
        val names = listOf("orders-archive-value", "orders-value", "orders-value-v2")

        assertEquals(listOf("orders-value", "orders-archive-value", "orders-value-v2"), order(names))
    }

    @Test
    fun `should list the schemas that are named after the topic before the others`() {
        val names = listOf("audit-value", "orders-archive-value", "payments-value")

        assertEquals(listOf("orders-archive-value", "audit-value", "payments-value"), order(names))
    }

    @Test
    fun `should not take the schema of another topic with the same beginning for the topic`() {
        val names = listOf("audit-value", "orders2-value", "orders-value")

        assertEquals(listOf("orders-value", "audit-value", "orders2-value"), order(names))
    }

    @Test
    fun `should match the topic by its exact name and not regardless of the case`() {
        val names = listOf("Orders-value", "audit-value", "orders-value")

        assertEquals(listOf("orders-value", "audit-value", "Orders-value"), order(names))
    }

    @Test
    fun `should list the pinned schemas after the ones of the topic and before the others`() {
        val names = listOf("a-value", "orders-value", "z-value", "m-value")

        assertEquals(
            listOf("orders-value", "z-value", "a-value", "m-value"),
            order(names, pinned = listOf("z-value"))
        )
    }

    @Test
    fun `should list the pinned schemas alphabetically`() {
        val names = listOf("d-value", "c-value", "b-value", "a-value")

        assertEquals(
            listOf("b-value", "d-value", "a-value", "c-value"),
            order(names, pinned = listOf("d-value", "b-value"))
        )
    }

    @Test
    fun `should list the pinned schema of the topic as the schema of the topic`() {
        val names = listOf("z-value", "orders-value")

        assertEquals(listOf("orders-value", "z-value"), order(names, pinned = listOf("orders-value", "z-value")))
    }

    @Test
    fun `should list the internal schemas last`() {
        val names = listOf("_schemas", "a-value", "_confluent-value", "b-value")

        assertEquals(listOf("a-value", "b-value", "_confluent-value", "_schemas"), order(names))
    }

    @Test
    fun `should list a pinned internal schema among the pinned ones`() {
        val names = listOf("_internal", "a-value", "b-value")

        assertEquals(listOf("_internal", "a-value", "b-value"), order(names, pinned = listOf("_internal")))
    }

    @Test
    fun `should take the schemas of a context for what they are called without it`() {
        val names = listOf(":.ctx:_internal", ":.ctx:a-value", ":.ctx:orders-archive-value", "b-value")

        assertEquals(
            listOf(":.ctx:orders-archive-value", ":.ctx:a-value", "b-value", ":.ctx:_internal"),
            order(names)
        )
    }

    @Test
    fun `should keep the order of names that differ only by their case the same`() {
        val names = listOf("b-value", "B-value", "a-value")

        assertEquals(order(names), order(names.reversed()))
        assertEquals(listOf("a-value", "B-value", "b-value"), order(names))
    }

    @Test
    fun `should not leave anything out`() {
        val names = (1..300).map { "subject-$it-value" } + listOf("_internal", "orders-value", "orders-key")

        val ordered = order(names, pinned = listOf("subject-7-value"))

        assertEquals(names.sorted(), ordered.sorted())
        assertEquals(listOf("orders-value", "orders-key", "subject-7-value"), ordered.take(3))
        assertEquals("_internal", ordered.last())
    }
}
