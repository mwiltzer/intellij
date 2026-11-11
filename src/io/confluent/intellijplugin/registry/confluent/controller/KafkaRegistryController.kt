package io.confluent.intellijplugin.registry.confluent.controller

import com.intellij.icons.AllIcons
import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.project.DumbAwareToggleAction
import com.intellij.openapi.project.Project
import com.intellij.ui.DocumentAdapter
import com.intellij.ui.SearchTextField
import com.intellij.ui.SimpleTextAttributes
import com.intellij.util.ui.StatusText
import io.confluent.intellijplugin.core.monitoring.data.listener.DataModelListener
import io.confluent.intellijplugin.core.monitoring.data.model.FilterAdapter
import io.confluent.intellijplugin.core.monitoring.data.model.FilterKey
import io.confluent.intellijplugin.core.monitoring.data.model.ObjectDataModel
import io.confluent.intellijplugin.core.monitoring.table.DataTable
import io.confluent.intellijplugin.core.monitoring.toolwindow.AbstractTableController
import io.confluent.intellijplugin.core.monitoring.toolwindow.MainTreeController
import io.confluent.intellijplugin.core.settings.ColumnVisibilitySettings
import io.confluent.intellijplugin.core.table.renderers.FavoriteRenderer
import io.confluent.intellijplugin.core.table.renderers.LinkRenderer
import io.confluent.intellijplugin.core.ui.CustomComponentActionImpl
import io.confluent.intellijplugin.core.ui.filter.CountFilterPopupComponent
import io.confluent.intellijplugin.data.BaseClusterDataManager
import io.confluent.intellijplugin.registry.KafkaRegistryAddSchemaDialog
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.rfs.KafkaDriver
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import io.confluent.intellijplugin.toolwindow.NavigableController
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import com.intellij.ui.components.JBPanelWithEmptyText
import com.intellij.util.ui.JBUI
import io.confluent.intellijplugin.data.ClusterDataFilters
import io.confluent.intellijplugin.data.HiddenSchemas
import java.awt.BorderLayout
import java.awt.CardLayout
import javax.swing.JComponent
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.ListSelectionModel
import javax.swing.event.DocumentEvent

internal class KafkaRegistryController(
    val project: Project,
    val dataManager: BaseClusterDataManager,
    private val mainController: NavigableController
) : AbstractTableController<KafkaSchemaInfo>() {
    val registryType = dataManager.registryType
    private val model: ObjectDataModel<KafkaSchemaInfo> = dataManager.schemaRegistryModel!!

    private val modelListener = object : DataModelListener {
        override fun onChanged() {
            updateUIForEmptyState()
        }

        override fun onError(msg: String, e: Throwable?) {
            updateUIForEmptyState()
        }
    }

    private val cardLayout = CardLayout()
    private val wrapperPanel = JPanel(cardLayout)
    private val emptyPanel = JBPanelWithEmptyText(BorderLayout())

    private val searchTextField: SearchTextField = SearchTextField(false).apply {
        addDocumentListener(object : DocumentAdapter() {
            override fun textChanged(e: DocumentEvent) {
                val config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(dataManager.getSchemaRegistryConfigId())
                config.schemaFilterName = this@apply.text
                dataManager.refilterSchemas()
            }
        })

        text = KafkaToolWindowSettings.getInstance().getOrCreateConfig(dataManager.getSchemaRegistryConfigId()).schemaFilterName ?: ""
    }

    private val showFavoriteSchemasAction = object : DumbAwareToggleAction(
        KafkaMessagesBundle.message("action.show.favorite.schemas"), null,
        AllIcons.Nodes.Favorite
    ) {
        override fun isSelected(e: AnActionEvent) = KafkaToolWindowSettings.getInstance().showFavoriteSchema
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun setSelected(e: AnActionEvent, state: Boolean) {
            KafkaToolWindowSettings.getInstance().showFavoriteSchema = state
            dataManager.refilterSchemas()
        }
    }

    private val showInternalSchemasAction = object : DumbAwareToggleAction(
        KafkaMessagesBundle.message("action.show.internal.schemas"),
        KafkaMessagesBundle.message("action.show.internal.schemas.description"),
        AllIcons.Actions.ToggleVisibility
    ) {
        override fun isSelected(e: AnActionEvent) = KafkaToolWindowSettings.getInstance().showInternalSchemas
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun setSelected(e: AnActionEvent, state: Boolean) {
            KafkaToolWindowSettings.getInstance().showInternalSchemas = state
            dataManager.refilterSchemas()
        }
    }

    private val showOtherTopicsSchemasAction = object : DumbAwareToggleAction(
        KafkaMessagesBundle.message("action.show.other.topics.schemas"),
        KafkaMessagesBundle.message("action.show.other.topics.schemas.description"),
        AllIcons.General.Filter
    ) {
        override fun isSelected(e: AnActionEvent) = KafkaToolWindowSettings.getInstance().showOtherTopicsSchemas
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun setSelected(e: AnActionEvent, state: Boolean) {
            KafkaToolWindowSettings.getInstance().showOtherTopicsSchemas = state
            dataManager.refilterSchemas()
        }
    }

    // Tells that schemas are hidden, so that nobody looks for one that is not there.
    private val hiddenNote = JLabel().apply {
        foreground = JBUI.CurrentTheme.Label.disabledForeground(false)
        icon = AllIcons.General.Note
        isVisible = false
    }

    init {
        init()

        dataTable.selectionModel.selectionMode = ListSelectionModel.MULTIPLE_INTERVAL_SELECTION

        dataTable.customDataProvider = UiDataProvider { sink ->
            sink[MainTreeController.DATA_MANAGER] = dataManager
            sink[MainTreeController.RFS_PATH] =
                getSelectedItem()?.name?.let { KafkaDriver.schemasPath.child(it, false) }
        }

        wrapperPanel.add(emptyPanel, "EMPTY")
        wrapperPanel.add(super.getComponent(), "TABLE")

        model.addListener(modelListener)

        updateUIForEmptyState()
    }

    override fun dispose() {
        model.removeListener(modelListener)
        super.dispose()
    }

    override fun getComponent() = wrapperPanel

    private fun updateUIForEmptyState() {
        val settings = KafkaToolWindowSettings.getInstance()
        val config = settings.getOrCreateConfig(dataManager.getSchemaRegistryConfigId())
        val hidden = dataManager.hiddenSchemas
        val shown = dataManager.getSchemas().size

        updateHiddenNote(hidden)

        // Schemas that are only hidden, by a filter or by a setting, are no reason to ask for the first one.
        if (ClusterDataFilters.isRegistryEmpty(shown, config.schemaFilterName, settings.showFavoriteSchema, hidden)) {
            updateEmptyPanel()
            cardLayout.show(wrapperPanel, "EMPTY")
        } else {
            cardLayout.show(wrapperPanel, "TABLE")
            (decoratedTableComponent.layout as? BorderLayout)
                ?.getLayoutComponent(BorderLayout.NORTH)?.isVisible = true
            dataTable.tableHeader?.isVisible = true
            if (shown == 0) updateEmptyText()
        }
    }

    private fun updateHiddenNote(hidden: HiddenSchemas) {
        val parts = listOfNotNull(
            hidden.internal.takeIf { it > 0 }?.let { KafkaMessagesBundle.message("schemas.hidden.internal", it) },
            hidden.otherTopics.takeIf { it > 0 }?.let { KafkaMessagesBundle.message("schemas.hidden.other.topics", it) }
        )
        hiddenNote.isVisible = parts.isNotEmpty()
        hiddenNote.text = KafkaMessagesBundle.message("schemas.hidden.note", parts.joinToString(", "))
    }

    private fun updateEmptyPanel() {
        emptyPanel.emptyText.clear()
        emptyPanel.emptyText.appendText(KafkaMessagesBundle.message("schemas.empty.text"))
        emptyPanel.emptyText.appendLine(
            KafkaMessagesBundle.message("schemas.empty.text.create.link"),
            SimpleTextAttributes.LINK_ATTRIBUTES
        ) {
            KafkaRegistryAddSchemaDialog(project, dataManager).show()
        }
    }

    private fun updateEmptyText() {
        val settings = KafkaToolWindowSettings.getInstance()
        val clusterConfig = settings.getOrCreateConfig(dataManager.getSchemaRegistryConfigId())
        val hidden = dataManager.hiddenSchemas
        val filtered = !clusterConfig.schemaFilterName.isNullOrBlank() || settings.showFavoriteSchema

        dataTable.emptyText.clear()
        if (filtered || hidden.total == 0) {
            dataTable.emptyText.appendText(
                KafkaMessagesBundle.message("schemas.empty.text.filter"),
                StatusText.DEFAULT_ATTRIBUTES
            )
            dataTable.emptyText.appendSecondaryText(
                KafkaMessagesBundle.message("topics.empty.text.filter.additional"),
                SimpleTextAttributes.LINK_PLAIN_ATTRIBUTES
            ) {
                clusterConfig.schemaFilterName = null
                settings.showFavoriteSchema = false
                searchTextField.text = ""
                dataManager.refilterSchemas()
            }
        } else {
            dataTable.emptyText.appendText(
                KafkaMessagesBundle.message("schemas.empty.text.hidden"),
                StatusText.DEFAULT_ATTRIBUTES
            )
        }

        // What is hidden is one click away.
        if (hidden.internal > 0) {
            dataTable.emptyText.appendLine(
                KafkaMessagesBundle.message("schemas.empty.text.show.internal", hidden.internal),
                SimpleTextAttributes.LINK_PLAIN_ATTRIBUTES
            ) {
                settings.showInternalSchemas = true
                dataManager.refilterSchemas()
            }
        }
        if (hidden.otherTopics > 0) {
            dataTable.emptyText.appendLine(
                KafkaMessagesBundle.message("schemas.empty.text.show.other.topics", hidden.otherTopics),
                SimpleTextAttributes.LINK_PLAIN_ATTRIBUTES
            ) {
                settings.showOtherTopicsSchemas = true
                dataManager.refilterSchemas()
            }
        }
    }

    override fun customTableInit(table: DataTable<KafkaSchemaInfo>) {
        FavoriteRenderer.installOnColumn(table, columnModel.getColumn(0)).apply {
            onClick = { row, _ ->
                val schemaInfo = table.getDataAt(row)
                schemaInfo?.let { dataManager.updatePinnedSchemas(it.name, !it.isFavorite) }
            }
        }

        LinkRenderer.installOnColumn(table, columnModel.getColumn(1)).apply {
            onClick = { row, _ ->
                val schema = table.getDataAt(row)?.name
                schema?.let {
                    // Use dataManager to get proper schema path (Kafka vs CCloud)
                    mainController.open(dataManager.getSchemaPath(it))
                }
            }
        }
    }

    override fun createTopLeftToolbarActions(): List<AnAction> {

        val countFilter = CountFilterPopupComponent(
            KafkaMessagesBundle.message("label.filter.limit"),
            KafkaToolWindowSettings.getInstance().getOrCreateConfig(
                dataManager.getSchemaRegistryConfigId()
            ).registryLimit
        )
        FilterAdapter.install(dataTable.tableModel, countFilter, LIMIT_FILTER) { limit ->
            val config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(dataManager.getSchemaRegistryConfigId())
            config.registryLimit = limit
            dataManager.refilterSchemas()
        }

        return listOfNotNull(
            CustomComponentActionImpl(searchTextField),
            CustomComponentActionImpl(countFilter),
            showFavoriteSchemasAction,
            // Subjects are a thing of a Confluent registry, the schemas of Glue have no such names.
            showInternalSchemasAction.takeIf { registryType == KafkaRegistryType.CONFLUENT },
            showOtherTopicsSchemasAction.takeIf { registryType == KafkaRegistryType.CONFLUENT }
        )
    }

    override fun createStatusNotes(): List<JComponent> = listOf(hiddenNote)

    override fun createTopRightToolbarActions(): List<AnAction> {
        val actionManager = ActionManager.getInstance()
        val group = actionManager.getAction("Kafka.Schema.Actions") as DefaultActionGroup
        return group.getChildren(actionManager).toList()
    }

    override fun getAdditionalContextActions(): List<AnAction> {
        val actionManager = ActionManager.getInstance()
        val group = actionManager.getAction("Kafka.Schema.Actions") as DefaultActionGroup
        return group.getChildren(actionManager).toList()
    }

    override fun getColumnSettings(): ColumnVisibilitySettings = when (registryType) {
        KafkaRegistryType.NONE -> error("Should not be invoked")
        KafkaRegistryType.CONFLUENT -> KafkaToolWindowSettings.getInstance().confluentSchemaTableColumnSettings
        KafkaRegistryType.AWS_GLUE -> KafkaToolWindowSettings.getInstance().glueSchemaTableColumnSettings
    }

    override fun showColumnFilter(): Boolean = false

    override fun getRenderableColumns() = KafkaSchemaInfo.renderableColumns
    override fun getDataModel() = model

    companion object {
        val LIMIT_FILTER = FilterKey("registryLimit")
    }
}


