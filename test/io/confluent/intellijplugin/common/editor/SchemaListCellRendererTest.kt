package io.confluent.intellijplugin.common.editor

import com.intellij.testFramework.junit5.TestApplication
import com.intellij.ui.SimpleColoredComponent
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.awt.Container
import java.awt.Dimension
import java.awt.image.BufferedImage
import javax.swing.JList
import javax.swing.JScrollPane
import javax.swing.SwingUtilities

@TestApplication
class SchemaListCellRendererTest {

    private val wanted = mutableListOf<String>()
    private var listing = false

    private val renderer = SchemaListCellRenderer(isListing = { listing }, formatWanted = { wanted += it.schemaName })

    private fun schemas(count: Int, withFormat: Set<Int> = emptySet()) = (0 until count).map {
        RegistrySchemaInEditor("schema-%03d".format(it), KafkaRegistryFormat.AVRO.takeIf { _ -> it in withFormat })
    }

    private fun <T> onEdt(block: () -> T): T {
        var result: T? = null
        SwingUtilities.invokeAndWait { result = block() }
        @Suppress("UNCHECKED_CAST")
        return result as T
    }

    private class Showing(val list: JList<RegistrySchemaInEditor>, val pane: JScrollPane)

    // A list in a scroll pane that shows about ten rows, laid out as Swing does when it is on screen.
    private fun show(items: List<RegistrySchemaInEditor>, list: JList<RegistrySchemaInEditor> = JList()): Showing = onEdt {
        list.setListData(items.toTypedArray())
        list.cellRenderer = renderer
        val pane = JScrollPane(list)
        pane.size = Dimension(300, 200)
        layoutAll(pane)
        Showing(list, pane)
    }

    private fun layoutAll(container: Container) {
        container.doLayout()
        container.components.forEach { if (it is Container) layoutAll(it) }
    }

    private fun Showing.paint() = onEdt {
        val image = BufferedImage(pane.width, pane.height, BufferedImage.TYPE_INT_ARGB)
        val graphics = image.createGraphics()
        try {
            pane.paint(graphics)
        } finally {
            graphics.dispose()
        }
    }

    private fun Showing.visibleNames() = onEdt {
        (list.firstVisibleIndex..list.lastVisibleIndex).map { list.model.getElementAt(it).schemaName }
    }

    private fun textOf(component: java.awt.Component) = (component as SimpleColoredComponent).getCharSequence(false).toString()

    @Test
    fun `should ask for the formats of the rows on screen`() {
        val showing = show(schemas(200))

        showing.paint()

        val visible = showing.visibleNames()
        assertTrue(visible.size in 5..15, visible.toString())
        assertEquals(visible, wanted.distinct())
    }

    @Test
    fun `should not ask for the rows that are out of view`() {
        val showing = show(schemas(200))

        showing.paint()

        assertFalse("schema-199" in wanted)
        assertFalse("schema-100" in wanted)
    }

    @Test
    fun `should not ask for any row while the list is only measured`() {
        val list = onEdt { JList<RegistrySchemaInEditor>().also { it.setListData(schemas(200).toTypedArray()) } }
        onEdt { list.cellRenderer = renderer }

        // Swing draws every row once to learn how big the list is, which is no reason to look up 200 formats.
        onEdt { list.preferredSize }

        assertTrue(wanted.isEmpty(), wanted.toString())
    }

    @Test
    fun `should ask for the rows that are scrolled into view`() {
        val showing = show(schemas(200))
        showing.paint()
        wanted.clear()

        onEdt {
            showing.list.ensureIndexIsVisible(150)
            layoutAll(showing.pane)
        }
        showing.paint()

        assertTrue("schema-150" in wanted, wanted.toString())
        assertEquals(showing.visibleNames(), wanted.distinct())
    }

    @Test
    fun `should draw a list that has to measure its rows while it is drawn`() {
        val showing = show(schemas(200))
        // A list that changes measures all its rows again at the next draw. Looking up which rows are on screen
        // asks for that measurement, which draws the rows, which looks up the rows on screen...
        onEdt { showing.list.setListData(schemas(300).toTypedArray()) }

        showing.paint()

        assertEquals(showing.visibleNames(), wanted.distinct())
    }

    @Test
    fun `should not ask for a row that has its format`() {
        val showing = show(schemas(200, withFormat = (0..4).toSet()))

        showing.paint()

        assertTrue(wanted.none { it in setOf("schema-000", "schema-001", "schema-002", "schema-003", "schema-004") }, wanted.toString())
        assertTrue("schema-005" in wanted, wanted.toString())
    }

    @Test
    fun `should not ask for the schema that the combo box shows as its value`() {
        val showing = show(schemas(3))

        // A combo box draws its selected item with the index -1.
        onEdt { renderer.getListCellRendererComponent(showing.list, schemas(1).first(), -1, false, false) }

        assertTrue(wanted.isEmpty(), wanted.toString())
    }

    @Test
    fun `should draw the name and the format`() {
        val showing = show(schemas(3, withFormat = setOf(0)))

        val withFormat = onEdt { renderer.getListCellRendererComponent(showing.list, showing.list.model.getElementAt(0), 0, false, false) }
        assertEquals("schema-000  ${KafkaRegistryFormat.AVRO.presentable}", textOf(withFormat).trim())

        val withoutFormat = onEdt { renderer.getListCellRendererComponent(showing.list, showing.list.model.getElementAt(1), 1, false, false) }
        assertEquals("schema-001", textOf(withoutFormat).trim())
    }

    @Test
    fun `should say that it is loading when there is nothing to draw and the names are listed`() {
        val showing = show(emptyList())

        listing = true
        val whileListing = onEdt { renderer.getListCellRendererComponent(showing.list, null, -1, false, false) }
        assertEquals(KafkaMessagesBundle.message("registry.schemas.loading"), textOf(whileListing))

        listing = false
        val afterwards = onEdt { renderer.getListCellRendererComponent(showing.list, null, -1, false, false) }
        assertEquals("", textOf(afterwards))
    }

    @Test
    fun `should repaint the list that it drew last`() {
        var repaints = 0
        val list = object : JList<RegistrySchemaInEditor>() {
            override fun repaint() {
                repaints++
            }
        }
        val showing = show(schemas(20), list)
        showing.paint()
        val before = repaints

        renderer.repaintList()

        assertEquals(before + 1, repaints)
    }

    @Test
    fun `should have nothing to repaint before it drew a list`() {
        renderer.repaintList()
    }
}
