package io.confluent.intellijplugin.common.editor

import com.intellij.ui.ColoredListCellRenderer
import com.intellij.ui.SimpleTextAttributes
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import java.lang.ref.WeakReference
import javax.swing.JList

/**
 * Draws a schema of the schema combo box: its name and, once it is known, its format.
 *
 * A registry can hold thousands of schemas, and the format of each costs a registry request. So only the rows that
 * are on screen ask for a format they do not have yet ([formatWanted]), and no others. The rows that Swing draws
 * only to measure the list, or that are scrolled out of view, do not.
 */
internal class SchemaListCellRenderer(
    private val isListing: () -> Boolean,
    private val formatWanted: (RegistrySchemaInEditor) -> Unit
) : ColoredListCellRenderer<RegistrySchemaInEditor>() {

    private var lastList = WeakReference<JList<*>>(null)

    // Set while the visible rows are looked up, see [isOnScreen]. The renderer is used on the EDT only.
    private var lookingUp = false

    override fun customizeCellRenderer(
        list: JList<out RegistrySchemaInEditor>,
        value: RegistrySchemaInEditor?,
        index: Int,
        selected: Boolean,
        hasFocus: Boolean
    ) {
        if (value == null) {
            if (isListing())
                append(KafkaMessagesBundle.message("registry.schemas.loading"), SimpleTextAttributes.GRAYED_ATTRIBUTES)
            return
        }

        if (index >= 0) {
            lastList = WeakReference(list)
            if (value.schemaFormat == null && isOnScreen(list, index)) formatWanted(value)
        }

        append(value.schemaName + "  ")
        append(value.schemaFormat?.presentable ?: "", SimpleTextAttributes.GRAYED_ATTRIBUTES)
    }

    /** Draws the rows again, e.g. because formats arrived while the list is open. */
    fun repaintList() {
        lastList.get()?.repaint()
    }

    // The visible rows of a list that is not on screen are none (-1 .. -1).
    private fun isOnScreen(list: JList<*>, index: Int): Boolean {
        // Without an area to show, no row is on screen, and the list need not be asked, which costs a layout.
        if (list.visibleRect.isEmpty) return false
        // Finding the visible rows lays the list out when that is due, and that draws every row to measure it, back
        // through here. The rows drawn on the way are measured, not shown.
        if (lookingUp) return false
        lookingUp = true
        try {
            return index in list.firstVisibleIndex..list.lastVisibleIndex
        } finally {
            lookingUp = false
        }
    }
}
