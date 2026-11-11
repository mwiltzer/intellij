package io.confluent.intellijplugin.producer.editor

import com.intellij.openapi.Disposable
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.util.Disposer
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.core.ui.CustomListCellRenderer
import io.confluent.intellijplugin.core.util.invokeLater
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.awt.event.ItemEvent
import javax.swing.event.PopupMenuEvent
import javax.swing.event.PopupMenuListener

/** An entry of the list of versions. `null` is the latest version. */
internal data class SchemaVersionChoice(val version: Long?)

/**
 * The combo box with which the producer chooses the version of the registry schema it writes with. It follows the
 * schema of [schemaComboBox], and asks the registry for the versions of that schema when its list is opened.
 * The logic is in [SchemaVersionSelector].
 *
 * @param loadVersions the versions of a schema, or `null` when the registry could not be asked
 * @param onChosen is told when somebody chose a version
 */
internal class SchemaVersionComboBox(
    parent: Disposable,
    private val schemaComboBox: ComboBox<RegistrySchemaInEditor>,
    loadVersions: suspend (schemaName: String) -> List<Long>?,
    private val onChosen: () -> Unit
) {
    val comboBox = ComboBox<SchemaVersionChoice>()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default).also { scope ->
        Disposer.register(parent) { scope.cancel() }
    }

    private val selector = SchemaVersionSelector(scope, loadVersions) { state -> invokeLater { show(state) } }

    // Rebuilding the list selects things on the way, which is no choice.
    private var updating = false

    // The latest version, as the registry told, for the first entry to name it.
    private var latest: Long? = null

    init {
        comboBox.isSwingPopup = false
        comboBox.prototypeDisplayValue = SchemaVersionChoice(null)
        comboBox.toolTipText = KafkaMessagesBundle.message("producer.schema.version.tooltip")
        comboBox.renderer = CustomListCellRenderer<SchemaVersionChoice> { choice -> label(choice) }

        comboBox.addItemListener {
            if (it.stateChange == ItemEvent.SELECTED && !updating) {
                selector.choose((comboBox.item as? SchemaVersionChoice)?.version)
                onChosen()
            }
        }

        comboBox.addPopupMenuListener(object : PopupMenuListener {
            override fun popupMenuWillBecomeVisible(e: PopupMenuEvent?) = selector.reload()
            override fun popupMenuWillBecomeInvisible(e: PopupMenuEvent?) = Unit
            override fun popupMenuCanceled(e: PopupMenuEvent?) = Unit
        })

        schemaComboBox.addItemListener { selector.schemaSelected(schemaComboBox.item?.schemaName) }

        show(selector.state())
        selector.schemaSelected(schemaComboBox.item?.schemaName)
    }

    /** The version a record of the schema [schemaName] is written with, `null` for the latest one. */
    fun selectedFor(schemaName: String?): Long? = selector.selectedFor(schemaName)

    /** Brings back the version that was saved for the schema [schemaName]. */
    fun restore(schemaName: String, version: Long?) = selector.restore(schemaName, version)

    // The numbers are given as text: they are not amounts, and must not come out as "1,234".
    private fun label(choice: SchemaVersionChoice): String {
        val version = choice.version
        val latest = latest
        return when {
            version != null -> KafkaMessagesBundle.message("producer.schema.version.number", version.toString())
            latest != null -> KafkaMessagesBundle.message("producer.schema.version.latest.number", latest.toString())
            else -> KafkaMessagesBundle.message("producer.schema.version.latest")
        }
    }

    private fun show(state: SchemaVersionState) {
        updating = true
        try {
            latest = state.latest
            val choices = state.choices.map { SchemaVersionChoice(it) }
            val shown = (0 until comboBox.model.size).map { comboBox.model.getElementAt(it) }
            if (shown != choices) {
                comboBox.removeAllItems()
                choices.forEach { comboBox.addItem(it) }
            }
            comboBox.item = SchemaVersionChoice(state.selected)
        } finally {
            updating = false
        }
        comboBox.isEnabled = state.schemaName != null
        comboBox.repaint()
    }
}
