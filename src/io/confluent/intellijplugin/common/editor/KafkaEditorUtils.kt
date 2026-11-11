package io.confluent.intellijplugin.common.editor

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.google.protobuf.Message
import com.intellij.json.JsonLanguage
import com.intellij.lang.Language
import com.intellij.openapi.Disposable
import com.intellij.openapi.application.runInEdt
import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.progress.ProcessCanceledException
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.ValidationInfo
import com.intellij.openapi.util.Disposer
import com.intellij.ui.EditorCustomization
import com.intellij.ui.EditorTextFieldProvider
import com.intellij.util.ui.UIUtil
import io.confluent.intellijplugin.common.models.KafkaFieldType
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.common.models.TopicInEditor
import io.confluent.intellijplugin.core.monitoring.data.listener.DataModelListener
import io.confluent.intellijplugin.core.settings.getValidator
import io.confluent.intellijplugin.core.settings.withValidator
import io.confluent.intellijplugin.core.ui.ComponentColoredBorder
import io.confluent.intellijplugin.core.ui.CustomListCellRenderer
import io.confluent.intellijplugin.core.ui.DarculaTextAreaBorder
import io.confluent.intellijplugin.core.util.executeNotOnEdt
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import io.confluent.intellijplugin.core.util.invokeLater
import io.confluent.intellijplugin.data.BaseClusterDataManager
import io.confluent.intellijplugin.data.SchemaFormats
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils
import org.apache.avro.generic.GenericData
import java.awt.event.ItemEvent.SELECTED
import java.nio.charset.Charset
import java.util.*
import javax.swing.BorderFactory

object KafkaEditorUtils {
    fun createTextArea(
        project: Project,
        language: Language = JsonLanguage.INSTANCE,
        additionalCustomization: List<EditorCustomization> = emptyList()
    ) =
        EditorTextFieldProvider.getInstance()
            .getEditorField(
                language, project,
                listOf(EditorCustomization {
                    it.settings.apply {
                        isLineNumbersShown = false
                        isLineMarkerAreaShown = false
                        isFoldingOutlineShown = false
                        isRightMarginShown = false
                        additionalLinesCount = 0
                        additionalColumnsCount = 0
                        isAdditionalPageAtBottom = false
                        isShowIntentionBulb = false
                    }
                }) + additionalCustomization
            ).apply {
                border = BorderFactory.createCompoundBorder(DarculaTextAreaBorder(), ComponentColoredBorder(3, 5, 3, 5))
                background = UIUtil.getTextFieldBackground()
                autoscrolls = false
                setCaretPosition(0)
            }

    fun getValueAsString(type: KafkaFieldType, value: Any?, format: KafkaRegistryFormat): String = when {
        value == null -> ""
        type == KafkaFieldType.BASE64 && value is ByteArray -> try {
            Base64.getEncoder().withoutPadding().encodeToString(value)
        } catch (e: Exception) {
            value.toString()
        }

        type == KafkaFieldType.MESSAGEPACK -> try {
            tryFormatJson(if (value is ByteArray) String(value, Charsets.UTF_8) else value.toString())
        } catch (e: Exception) {
            value.toString()
        }

        type == KafkaFieldType.JSON -> tryFormatJson(value.toString())
        type == KafkaFieldType.PROTOBUF_CUSTOM -> try {
            val message = value as Message
            tryFormatJson(ProtobufSchemaUtils.toJson(message).toString(Charset.defaultCharset()))
        } catch (t: Throwable) {
            value.toString()
        }

        type == KafkaFieldType.SCHEMA_REGISTRY -> {
            when (format) {
                KafkaRegistryFormat.AVRO -> {
                    val avro = AvroSchemaUtils.toJson(value).toString(Charset.defaultCharset())
                    val name = (value as? GenericData.Record)?.schema?.fullName
                    tryFormatJson(avro, name)
                }

                KafkaRegistryFormat.PROTOBUF -> try {
                    val message = value as Message
                    tryFormatJson(ProtobufSchemaUtils.toJson(message).toString(Charset.defaultCharset()))
                } catch (t: Throwable) {
                    value.toString()
                }

                KafkaRegistryFormat.JSON -> {
                    val jsonString = when (value) {
                        is JsonDataWithSchema -> {
                            value.payload
                        }

                        is ObjectNode -> {
                            if (value.get("payload") != null)
                                JsonSchemaUtils.toJson(value.get("payload")).toString(Charset.defaultCharset())
                            else
                                JsonSchemaUtils.toJson(value).toString(Charset.defaultCharset())
                        }

                        else -> {
                            JsonSchemaUtils.toJson(value).toString(Charset.defaultCharset())
                        }
                    }
                    tryFormatJson(jsonString)
                }

                KafkaRegistryFormat.UNKNOWN -> value.toString()
            }
        }

        else -> value.toString()
    }

    internal class KafkaDataModelListener<T>(
        private val comboBox: ComboBox<T>,
        private val onListUpdate: (List<T>?) -> Unit = {},
        private val dataSupplier: () -> Pair<List<T>?, Int?>
    ) : DataModelListener {
        override fun onChangedNonEdt() {
            updateComboBox(comboBox, onListUpdate, dataSupplier)
        }

        override fun onError(msg: String, e: Throwable?) = executeNotOnEdt {
            updateComboBox(comboBox, onListUpdate, dataSupplier)
        }
    }

    fun createFieldTypeComboBox(
        topicCombobox: ComboBox<TopicInEditor>,
        dataManager: BaseClusterDataManager,
        isKey: Boolean,
        rootDisposable: Disposable,
        onChange: (ComboBox<KafkaFieldType>) -> Unit
    ): ComboBox<KafkaFieldType> {
        val fieldTypes = if (dataManager.registryType != KafkaRegistryType.NONE)
            KafkaFieldType.allValues
        else
            KafkaFieldType.defaultValues

        val defaultFieldType = if (isKey) KafkaFieldType.STRING else KafkaFieldType.JSON
        val fieldsCombobox = ComboBox(fieldTypes.toTypedArray<KafkaFieldType>()).apply<ComboBox<KafkaFieldType>> {
            renderer = CustomListCellRenderer<KafkaFieldType> { it.title }
            selectedItem = defaultFieldType
            addActionListener {
                onChange(this)
            }
        }

        if (dataManager.registryType != KafkaRegistryType.NONE) {
            val job = SupervisorJob()
            val scope = CoroutineScope(Dispatchers.Default + job)
            Disposer.register(rootDisposable) { job.cancel() }

            // Only the detection for the topic selected last counts, an earlier one may still be running.
            var detection: Job? = null
            fun startDetection(typeWithoutSchema: KafkaFieldType?) {
                val topic = topicCombobox.item?.name
                detection?.cancel()
                detection = scope.launch {
                    val schemaType = detectSchemaFieldType(
                        topic = topic,
                        isKey = isKey,
                        listSchemaNames = { dataManager.getSchemaNamesForEditor().orEmpty() },
                        getSchemaFormat = dataManager::getSchemaFormat
                    ) ?: typeWithoutSchema ?: return@launch
                    runInEdt {
                        fieldsCombobox.selectedItem = schemaType
                    }
                }
            }

            topicCombobox.addItemListener {
                if (it.stateChange != SELECTED)
                    return@addItemListener

                startDetection(typeWithoutSchema = KafkaFieldType.STRING)
            }

            startDetection(typeWithoutSchema = null)
        }

        return fieldsCombobox
    }

    fun createConsumerGroups(
        rootDisposable: Disposable,
        dataManager: BaseClusterDataManager,
        withEmpty: Boolean
    ): ComboBox<String> {
        val calcData = {
            val prefix = if (withEmpty) listOf("") else listOf()
            val cachedData = dataManager.consumerGroupsModel.data?.map { it.consumerGroup } ?: emptyList()
            prefix + cachedData
        }

        val comboBox = ComboBox((calcData()).toTypedArray())
        comboBox.isSwingPopup = false
        comboBox.prototypeDisplayValue = "       " // Field is set for limiting combobox width.
        comboBox.renderer = CustomListCellRenderer<String> { it }

        val listener = object : DataModelListener {
            override fun onChanged() = updateEditableComboBox(comboBox, calcData())
        }

        dataManager.consumerGroupsModel.addListener(listener)
        Disposer.register(rootDisposable) {
            dataManager.consumerGroupsModel.removeListener(listener)
        }
        comboBox.isEditable = true
        return comboBox
    }

    fun createTopicComboBox(rootDisposable: Disposable, dataManager: BaseClusterDataManager): ComboBox<TopicInEditor> {
        val topics = dataManager.getTopics()
        val topicComboBox = ComboBox(topics.map { it.toEditorTopic() }.sortedBy { it.name }.toTypedArray())
        topicComboBox.isSwingPopup = false
        topicComboBox.prototypeDisplayValue =
            TopicInEditor("Topic sample name") // Field is set for limiting combobox width.
        topicComboBox.renderer = CustomListCellRenderer<TopicInEditor> { it.name }

        val listener = KafkaDataModelListener(topicComboBox) {
            dataManager.loadTopicNames().map { it.toEditorTopic() }.sortedBy { it.name } to null
        }
        dataManager.topicModel.addListener(listener)
        Disposer.register(rootDisposable) {
            dataManager.topicModel.removeListener(listener)
        }

        topicComboBox.withValidator(rootDisposable) {
            val selectedItem = topicComboBox.selectedItem as? TopicInEditor
            if (selectedItem == null || selectedItem.name.isBlank())
                ValidationInfo(KafkaMessagesBundle.message("producer.error.topic.empty"), topicComboBox)
            else
                null
        }

        topicComboBox.getValidator()?.enableValidation()

        listener.onChangedNonEdt()

        return topicComboBox
    }

    fun createSchemaComboBox(
        rootDisposable: Disposable,
        kafkaManager: BaseClusterDataManager,
        topicComboBox: ComboBox<TopicInEditor>,
        isKey: Boolean
    ): ComboBox<RegistrySchemaInEditor> {
        val schemaCombobox = ComboBox(arrayOf<RegistrySchemaInEditor>())
        schemaCombobox.isSwingPopup = false
        // Field is set for limiting combobox width. A combo box is as wide as its widest item, and a registry can hold
        // thousands of schemas with long names, which pushes the buttons next to it out of the editor.
        schemaCombobox.prototypeDisplayValue = RegistrySchemaInEditor("Schema sample name")
        schemaCombobox.toolTipText = KafkaMessagesBundle.message("registry.subject.combobox.default.name")

        // Names come first and the formats follow, see SchemaComboBoxLoader. Without a registry there is nothing to list.
        val loader = if (kafkaManager.registryType != KafkaRegistryType.NONE) {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            Disposer.register(rootDisposable) { scope.cancel() }
            SchemaComboBoxLoader(schemaCombobox, kafkaManager, scope, isKey) { topicComboBox.item?.name }
        } else {
            schemaCombobox.renderer = SchemaListCellRenderer(isListing = { false }, formatWanted = {})
            null
        }

        if (loader != null) {
            val registryListener = object : DataModelListener {
                override fun onChangedNonEdt() = loader.reload()
                override fun onError(msg: String, e: Throwable?) = loader.reload()
            }
            kafkaManager.schemaRegistryModel?.addListener(registryListener)
            Disposer.register(rootDisposable) {
                kafkaManager.schemaRegistryModel?.removeListener(registryListener)
            }

            topicComboBox.addItemListener {
                if (it.stateChange == SELECTED)
                    loader.load(topicChanged = true)
            }

            loader.load()
        }

        kafkaManager.initRefreshSchemasIfRequired()
        schemaCombobox.withValidator(rootDisposable) {
            if (schemaCombobox.item == null)
                ValidationInfo(KafkaMessagesBundle.message("validator.notEmpty"))
            else
                null
        }
        return schemaCombobox
    }

    /**
     * Format of the schema selected in [comboBox].
     *
     * The list shows the names before it knows the formats, so the selected schema may not have its format yet.
     * This asks for it, which must be done before a format is used, e.g. to produce or to show consumed records.
     */
    suspend fun resolveSelectedFormat(
        comboBox: ComboBox<RegistrySchemaInEditor>,
        source: SchemaFormats
    ): KafkaRegistryFormat? {
        val item = comboBox.item ?: return null
        item.schemaFormat?.let { return it }
        return source.getSchemaFormat(item.schemaName)?.also { item.schemaFormat = it }
    }

    /**
     * Field type to suggest for the [topic]: the schema registry one when the registry has a schema for the topic.
     *
     * Costs the names of the schemas, which the editors list once for all of them, and the details of at most one of
     * them, so it stays cheap on big registries.
     */
    internal suspend fun detectSchemaFieldType(
        topic: String?,
        isKey: Boolean,
        listSchemaNames: suspend (topic: String) -> List<String>,
        getSchemaFormat: suspend (String) -> KafkaRegistryFormat?
    ): KafkaFieldType? {
        if (topic.isNullOrEmpty())
            return null

        val schemaName = findTopicSchemaName(listSchemaNames(topic), topic, isKey) ?: return null
        return try {
            if (getSchemaFormat(schemaName) != null) KafkaFieldType.SCHEMA_REGISTRY else null
        } catch (e: ProcessCanceledException) {
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            thisLogger().warn("Failed to detect the type of schema '$schemaName'", e)
            null
        }
    }

    /** Name of the schema that belongs to the [topic] by the naming convention of the registry, if there is one. */
    internal fun findTopicSchemaName(schemaNames: Collection<String>, topic: String, isKey: Boolean): String? {
        val confluentSchemaName = if (isKey) "$topic-key" else "$topic-value"
        if (confluentSchemaName in schemaNames)
            return confluentSchemaName
        if (!isKey && topic in schemaNames)
            return topic
        return null
    }

    /**
     * Selects the schema called [schemaName], matching by name only.
     *
     * A combo box silently ignores an item it does not contain, and a schema of the same name but with another
     * format (e.g. one that is not resolved yet) is not the same item. If the list does not hold the schema yet
     * because it is still loading, the schema is added to it, so the selection survives the refresh of the list.
     */
    fun selectSchemaByName(
        comboBox: ComboBox<RegistrySchemaInEditor>,
        schemaName: String,
        format: KafkaRegistryFormat?
    ) {
        if (schemaName.isEmpty())
            return

        val model = comboBox.model
        val known = (0 until model.size).map { model.getElementAt(it) }.firstOrNull { it.schemaName == schemaName }
        if (known != null) {
            comboBox.item = known
            return
        }

        val pending = RegistrySchemaInEditor(schemaName = schemaName, schemaFormat = format)
        comboBox.addItem(pending)
        comboBox.item = pending
    }

    fun <T> updateEditableComboBox(comboBox: ComboBox<T>, newData: List<T>) {
        val oldValues = (0 until comboBox.model.size).map {
            comboBox.model.getElementAt(it)
        }
        if (oldValues == newData) {
            return
        }
        val oldValue = comboBox.item
        comboBox.removeAllItems()
        newData.forEach {
            comboBox.addItem(it)
        }
        comboBox.item = oldValue

        comboBox.invalidate()
        comboBox.repaint()
    }

    fun <T> updateComboBox(
        comboBox: ComboBox<T>,
        onListUpdate: (List<T>?) -> Unit = {},
        dataSupplier: () -> Pair<List<T>?, Int?>
    ) = executeNotOnEdt {
        val oldElements = (0 until comboBox.model.size).map {
            comboBox.model.getElementAt(it)
        }
        val (newElements, selectedItemIndex) = dataSupplier()
        invokeLater {
            if (oldElements == newElements) {
                updateSelectedIndex(comboBox, selectedItemIndex)
                return@invokeLater
            }


            val previouslySelectedIndex = comboBox.selectedIndex
            val previouslySelected = if (previouslySelectedIndex >= 0 && previouslySelectedIndex < comboBox.model.size) {
                comboBox.model.getElementAt(previouslySelectedIndex)
            } else null

            comboBox.removeAllItems()
            newElements?.forEach {
                comboBox.addItem(it)
            }

            if (selectedItemIndex != null) {
                updateSelectedIndex(comboBox, selectedItemIndex)
            } else if (previouslySelected != null && newElements != null) {
                val restoredIndex = newElements.indexOf(previouslySelected)
                if (restoredIndex >= 0) {
                    comboBox.selectedIndex = restoredIndex
                }
            }

            comboBox.invalidate()
            comboBox.repaint()
            onListUpdate(newElements)
        }
    }

    private fun <T> updateSelectedIndex(
        comboBox: ComboBox<T>,
        selectedItemIndex: Int?
    ) {
        comboBox.selectedIndex = when {
            selectedItemIndex != null -> selectedItemIndex
            else -> comboBox.selectedIndex
        }
    }

    fun tryFormatJson(text: String, shemaName: String? = null, pretty: Boolean = false): String {
        if (!isJsonString(text))
            return text
        return try {
            val gsonBuilder = GsonBuilder().disableHtmlEscaping().serializeNulls()
            if (pretty) gsonBuilder.setPrettyPrinting()
            val gson = gsonBuilder.create()
            val parseString = JsonParser.parseString(text)
            try {
                shemaName?.let {
                    parseString.asJsonObject.addProperty("\$\$\$SchemaName\$\$\$", shemaName)
                }
            } catch (t: Throwable) {
                //Ignore
            }
            gson.toJson(parseString)
        } catch (e: Exception) {
            text
        }
    }

    fun isJsonString(text: String): Boolean {
        val first = text.firstOrNull { !Character.isWhitespace(it) } ?: return false
        val last = text.lastOrNull { !Character.isWhitespace(it) } ?: return false
        return first == '{' && last == '}' && text.contains(":") || first == '[' && last == ']'
    }
}