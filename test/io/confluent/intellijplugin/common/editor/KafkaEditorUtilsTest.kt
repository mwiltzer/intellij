package io.confluent.intellijplugin.common.editor

import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.util.Disposer
import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.common.models.TopicInEditor
import io.confluent.intellijplugin.data.BaseClusterDataManager
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@TestApplication
class KafkaEditorUtilsTest {

    private fun <T> createComboBox(items: List<T>): ComboBox<T> {
        val comboBox = ComboBox<T>()
        items.forEach { comboBox.addItem(it) }
        return comboBox
    }

    private fun <T> callUpdateComboBoxAndWait(
        comboBox: ComboBox<T>,
        dataSupplier: () -> Pair<List<T>?, Int?>
    ) {
        val latch = CountDownLatch(1)
        KafkaEditorUtils.updateComboBox(comboBox, { latch.countDown() }, dataSupplier)
        latch.await(5, TimeUnit.SECONDS)
    }

    @Nested
    inner class `updateComboBox` {

        @Test
        fun `should preserve selected item by identity when list is reordered`() {
            val comboBox = createComboBox(listOf("alpha", "beta", "gamma"))
            comboBox.selectedIndex = 1 // "beta"

            callUpdateComboBoxAndWait(comboBox) {
                listOf("gamma", "alpha", "beta") to null
            }

            assertEquals("beta", comboBox.selectedItem)
        }

        @Test
        fun `should preserve selected item when new items are added`() {
            val comboBox = createComboBox(listOf("alpha", "beta"))
            comboBox.selectedIndex = 1 // "beta"

            callUpdateComboBoxAndWait(comboBox) {
                listOf("alpha", "new-topic", "beta", "another-topic") to null
            }

            assertEquals("beta", comboBox.selectedItem)
        }

        @Test
        fun `should use selectedItemIndex when provided`() {
            val comboBox = createComboBox(listOf("alpha", "beta", "gamma"))
            comboBox.selectedIndex = 0 // "alpha"

            callUpdateComboBoxAndWait(comboBox) {
                listOf("gamma", "alpha", "beta") to 2
            }

            assertEquals("beta", comboBox.selectedItem)
        }

        @Test
        fun `should fall back to index 0 when previously selected item is removed`() {
            val comboBox = createComboBox(listOf("alpha", "beta", "gamma"))
            comboBox.selectedIndex = 1 // "beta"

            callUpdateComboBoxAndWait(comboBox) {
                listOf("alpha", "gamma", "delta") to null
            }

            // "beta" no longer exists, no explicit index — comboBox defaults to 0
            assertEquals("alpha", comboBox.selectedItem)
        }

        @Test
        fun `should not change selection when list is unchanged`() {
            val comboBox = createComboBox(listOf("alpha", "beta", "gamma"))
            comboBox.selectedIndex = 2 // "gamma"

            callUpdateComboBoxAndWait(comboBox) {
                listOf("alpha", "beta", "gamma") to null
            }

            assertEquals("gamma", comboBox.selectedItem)
        }
    }

    @Nested
    inner class `getValueAsString` {

        @Test
        fun `should decode messagepack byte array to compact json string`() {
            val json = """{"a":1,"b":"two"}"""

            val result = KafkaEditorUtils.getValueAsString(
                KafkaFieldType.MESSAGEPACK, json.toByteArray(), KafkaRegistryFormat.UNKNOWN
            )

            assertEquals(json, result)
        }

        @Test
        fun `should compact messagepack string with spaces around braces`() {
            val input = """ [ "a" ] """
            val expected = """["a"]"""

            val result = KafkaEditorUtils.getValueAsString(
                KafkaFieldType.MESSAGEPACK, input, KafkaRegistryFormat.UNKNOWN
            )

            assertEquals(expected, result)
        }

        @Test
        fun `should return empty string for null value`() {
            val result = KafkaEditorUtils.getValueAsString(
                KafkaFieldType.MESSAGEPACK, null, KafkaRegistryFormat.UNKNOWN
            )

            assertEquals("", result)
        }
    }

    @Nested
    inner class `findTopicSchemaName` {

        private val schemaNames = listOf("orders", "orders-key", "orders-value", "payments", "other-value")

        @Test
        fun `should find the value schema of a topic`() {
            assertEquals("orders-value", KafkaEditorUtils.findTopicSchemaName(schemaNames, "orders", isKey = false))
        }

        @Test
        fun `should find the key schema of a topic`() {
            assertEquals("orders-key", KafkaEditorUtils.findTopicSchemaName(schemaNames, "orders", isKey = true))
        }

        @Test
        fun `should fall back to a schema named like the topic for values`() {
            assertEquals("payments", KafkaEditorUtils.findTopicSchemaName(schemaNames, "payments", isKey = false))
        }

        @Test
        fun `should not fall back to a schema named like the topic for keys`() {
            assertNull(KafkaEditorUtils.findTopicSchemaName(schemaNames, "payments", isKey = true))
        }

        @Test
        fun `should find nothing when no schema belongs to the topic`() {
            assertNull(KafkaEditorUtils.findTopicSchemaName(schemaNames, "shipments", isKey = false))
        }
    }

    @Nested
    inner class `detectSchemaFieldType` {

        private val loadedSchemas = mutableListOf<String>()

        private suspend fun detect(
            topic: String?,
            schemaNames: List<String>,
            isKey: Boolean = false,
            getSchemaFormat: suspend (String) -> KafkaRegistryFormat? = { KafkaRegistryFormat.AVRO }
        ) = KafkaEditorUtils.detectSchemaFieldType(
            topic = topic,
            isKey = isKey,
            listSchemaNames = { schemaNames },
            getSchemaFormat = {
                loadedSchemas += it
                getSchemaFormat(it)
            }
        )

        @Test
        fun `should suggest schema registry when the topic has a schema`() = runBlocking<Unit> {
            assertEquals(KafkaFieldType.SCHEMA_REGISTRY, detect("orders", listOf("orders-value")))
        }

        @Test
        fun `should look at the schema of the topic only, however many schemas the registry has`() = runBlocking<Unit> {
            val schemaNames = (1..500).map { "subject-$it-value" } + "orders-value"

            val type = detect("orders", schemaNames)

            assertEquals(KafkaFieldType.SCHEMA_REGISTRY, type)
            assertEquals(listOf("orders-value"), loadedSchemas)
        }

        @Test
        fun `should detect the key schema of a topic`() = runBlocking<Unit> {
            val type = detect("orders", listOf("orders-value", "orders-key"), isKey = true)

            assertEquals(KafkaFieldType.SCHEMA_REGISTRY, type)
            assertEquals(listOf("orders-key"), loadedSchemas)
        }

        @Test
        fun `should detect nothing without a topic`() = runBlocking<Unit> {
            assertNull(detect(null, listOf("orders-value")))
            assertNull(detect("", listOf("-value")))
            assertTrue(loadedSchemas.isEmpty())
        }

        @Test
        fun `should detect nothing when the registry has no schema for the topic`() = runBlocking<Unit> {
            assertNull(detect("orders", listOf("payments-value")))
            assertTrue(loadedSchemas.isEmpty())
        }

        @Test
        fun `should detect nothing when the format of the schema is not known`() = runBlocking<Unit> {
            assertNull(detect("orders", listOf("orders-value")) { null })
        }

        @Test
        fun `should ask for the schema names of the topic`() = runBlocking<Unit> {
            val asked = mutableListOf<String>()

            val type = KafkaEditorUtils.detectSchemaFieldType(
                topic = "orders",
                isKey = false,
                listSchemaNames = { topic -> asked += topic; listOf("orders-value") },
                getSchemaFormat = { KafkaRegistryFormat.AVRO }
            )

            assertEquals(KafkaFieldType.SCHEMA_REGISTRY, type)
            assertEquals(listOf("orders"), asked)
        }

        @Test
        fun `should not ask for any names without a topic`() = runBlocking<Unit> {
            val asked = mutableListOf<String>()

            KafkaEditorUtils.detectSchemaFieldType(
                topic = null,
                isKey = false,
                listSchemaNames = { topic -> asked += topic; listOf("orders-value") },
                getSchemaFormat = { KafkaRegistryFormat.AVRO }
            )

            assertTrue(asked.isEmpty())
        }

        @Test
        fun `should detect nothing when the schema can not be loaded`() = runBlocking<Unit> {
            assertNull(detect("orders", listOf("orders-value")) { error("registry unavailable") })
        }

        @Test
        fun `should let cancellation through`() = runBlocking<Unit> {
            assertThrows<CancellationException> {
                detect("orders", listOf("orders-value")) { throw CancellationException("cancelled") }
            }
        }
    }

    @Nested
    inner class `selectSchemaByName` {

        private val orders = RegistrySchemaInEditor("orders-value", KafkaRegistryFormat.AVRO)
        private val payments = RegistrySchemaInEditor("payments-value", KafkaRegistryFormat.JSON)

        @Test
        fun `should select the schema by name although its format differs`() {
            // The list may still show a format that is not resolved yet, the saved config has the real one.
            val comboBox = createComboBox(
                listOf(
                    RegistrySchemaInEditor("orders-value", KafkaRegistryFormat.UNKNOWN),
                    RegistrySchemaInEditor("payments-value", KafkaRegistryFormat.UNKNOWN)
                )
            )

            KafkaEditorUtils.selectSchemaByName(comboBox, "payments-value", KafkaRegistryFormat.JSON)

            assertEquals(1, comboBox.selectedIndex)
            assertEquals(2, comboBox.model.size)
        }

        @Test
        fun `should add the schema to a list that does not have it yet`() {
            val comboBox = createComboBox(emptyList<RegistrySchemaInEditor>())

            KafkaEditorUtils.selectSchemaByName(comboBox, "orders-value", KafkaRegistryFormat.AVRO)

            assertEquals(orders, comboBox.item)
            assertEquals(1, comboBox.model.size)
        }

        @Test
        fun `should keep the selection when the list is loaded afterwards`() {
            val comboBox = createComboBox(emptyList<RegistrySchemaInEditor>())
            KafkaEditorUtils.selectSchemaByName(comboBox, "payments-value", KafkaRegistryFormat.JSON)

            // Same as the combo box of the editors does: prefer the schema that is selected by name.
            callUpdateComboBoxAndWait(comboBox) {
                val loaded = listOf(orders, payments)
                val preferred = comboBox.item?.schemaName
                loaded to loaded.indexOfFirst { it.schemaName == preferred }.takeIf { it >= 0 }
            }

            assertEquals(payments, comboBox.item)
            assertEquals(2, comboBox.model.size)
        }

        @Test
        fun `should ignore an empty name`() {
            val comboBox = createComboBox(listOf(orders, payments))
            comboBox.selectedIndex = 1

            KafkaEditorUtils.selectSchemaByName(comboBox, "", KafkaRegistryFormat.AVRO)

            assertEquals(1, comboBox.selectedIndex)
            assertEquals(2, comboBox.model.size)
        }
    }

    @Nested
    inner class `createSchemaComboBox` {

        private fun withSchemaComboBox(test: (ComboBox<RegistrySchemaInEditor>) -> Unit) {
            val parent = Disposer.newDisposable("schema combo box test")
            try {
                val manager = mock<BaseClusterDataManager> { on { registryType } doReturn KafkaRegistryType.NONE }
                test(KafkaEditorUtils.createSchemaComboBox(parent, manager, ComboBox(arrayOf<TopicInEditor>()), false))
            } finally {
                Disposer.dispose(parent)
            }
        }

        @Test
        fun `should not get wider with a long schema name`() = withSchemaComboBox { comboBox ->
            val emptyWidth = comboBox.preferredSize.width
            val emptyMinimum = comboBox.minimumSize.width

            comboBox.addItem(RegistrySchemaInEditor("a".repeat(300), KafkaRegistryFormat.AVRO))

            // The width of a combo box is the one of its widest item, which pushed the buttons next to it out of the editor.
            assertEquals(emptyWidth, comboBox.preferredSize.width)
            assertEquals(emptyMinimum, comboBox.minimumSize.width)
            assertTrue(emptyWidth < 300, "$emptyWidth")
        }

        @Test
        fun `should not get wider with many schemas`() = withSchemaComboBox { comboBox ->
            val emptyWidth = comboBox.preferredSize.width

            (1..2_000).forEach { comboBox.addItem(RegistrySchemaInEditor("com.example.events.SomeEvent$it.v1-value")) }

            assertEquals(emptyWidth, comboBox.preferredSize.width)
        }
    }
}
