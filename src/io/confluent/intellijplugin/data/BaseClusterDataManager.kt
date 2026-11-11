package io.confluent.intellijplugin.data

import com.intellij.CommonBundle
import com.intellij.openapi.Disposable
import com.intellij.openapi.application.EDT
import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.progress.ProcessCanceledException
import com.intellij.openapi.progress.runBlockingMaybeCancellable
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.util.Disposer
import com.intellij.util.concurrency.annotations.RequiresBackgroundThread
import io.confluent.intellijplugin.core.connection.updater.IntervalUpdateSettings
import io.confluent.intellijplugin.core.monitoring.data.MonitoringDataManager
import io.confluent.intellijplugin.core.monitoring.data.model.FieldGroupsData
import io.confluent.intellijplugin.core.monitoring.data.model.ObjectDataModel
import io.confluent.intellijplugin.core.monitoring.data.storage.FieldGroupsDataModelStorage
import io.confluent.intellijplugin.core.monitoring.data.storage.ObjectDataModelStorage
import io.confluent.intellijplugin.core.monitoring.rfs.MonitoringDriver
import io.confluent.intellijplugin.core.rfs.driver.RfsPath
import io.confluent.intellijplugin.core.rfs.util.RfsNotificationUtils
import io.confluent.intellijplugin.model.BdtTopicPartition
import io.confluent.intellijplugin.model.ConsumerGroupOffsetInfo
import io.confluent.intellijplugin.model.ConsumerGroupPresentable
import io.confluent.intellijplugin.model.TopicConfig
import io.confluent.intellijplugin.model.TopicPresentable
import io.confluent.intellijplugin.consumer.editor.KafkaConsumerPanelStorage
import io.confluent.intellijplugin.consumer.editor.KafkaConsumerSettings
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.SchemaVersionInfo
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import io.confluent.intellijplugin.util.catchFailures
import io.confluent.intellijplugin.util.timed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.jetbrains.concurrency.Promise

abstract class BaseClusterDataManager(
    project: Project?,
    settings: IntervalUpdateSettings,
    driverProvider: () -> MonitoringDriver
) : MonitoringDataManager(project, settings, driverProvider), SchemaFormats {

    abstract val connectionId: String
    abstract val registryType: KafkaRegistryType

    // The registry work behind the schemas must go on when the one who asked for it gives up, so it has its own scope.
    private val schemaScope: CoroutineScope by lazy {
        CoroutineScope(SupervisorJob() + Dispatchers.IO).also { scope ->
            // A manager that is disposed already takes no more work.
            if (!Disposer.tryRegister(this, Disposable { scope.cancel() })) scope.cancel()
        }
    }

    /** The formats of the registry schemas, shared by the tool window and the editors. */
    internal val schemaFormats: SchemaFormatResolver by lazy {
        SchemaFormatResolver(scope = schemaScope) { loadSchemaDetails(it) }
    }

    // The names of all the schemas, shared by the editors. Changes through the plugin invalidate them sooner than they
    // expire, and what is older is still shown by the editors while the registry is asked again, see
    // getCachedSchemaNames.
    private val schemaNames: ExpiringSingleFlight<List<String>> by lazy {
        ExpiringSingleFlight(schemaScope, SCHEMA_NAMES_TTL_MS) {
            thisLogger().timed("Listing the schema names of connection $connectionId") {
                listSchemasNames(limit = null, filter = null).first.map { it.name }.sortedBy { it.lowercase() }
            }
        }
    }

    /**
     * Get the RFS path for a schema subject.
     * Kafka: ["Schema Registry", schemaName]
     * CCloud: [schemaRegistryId, schemaName]
     */
    abstract fun getSchemaPath(schemaName: String): RfsPath

    /**
     * Get the config ID to use for all schema-related settings (favorites, filters, limits).
     * For Kafka: returns connectionId
     * For CCloud: returns schema registry ID (so all clusters sharing an SR see the same config)
     */
    open fun getSchemaRegistryConfigId(): String = connectionId

    val topicModel: ObjectDataModel<TopicPresentable> by lazy {
        createTopicsDataModel().also { Disposer.register(this, it) }
    }

    open val topicPartitionsModels: ObjectDataModelStorage<String, BdtTopicPartition> by lazy {
        createTopicPartitionsStorage().also { Disposer.register(this, it) }
    }

    val topicConfigsModels: ObjectDataModelStorage<String, TopicConfig> by lazy {
        createTopicConfigsStorage().also { Disposer.register(this, it) }
    }

    val consumerGroupsModel: ObjectDataModel<ConsumerGroupPresentable> by lazy {
        createConsumerGroupsDataModel().also { Disposer.register(this, it) }
    }

    val consumerGroupsOffsets: ObjectDataModelStorage<String, ConsumerGroupOffsetInfo> by lazy {
        createConsumerGroupOffsetsStorage().also { Disposer.register(this, it) }
    }

    val schemaRegistryModel: ObjectDataModel<KafkaSchemaInfo>? by lazy {
        createSchemaRegistryDataModel()?.also { Disposer.register(this, it) }
    }

    val schemaVersionModels: FieldGroupsDataModelStorage<String, List<Long>> by lazy {
        createSchemaVersionsStorage().also { Disposer.register(this, it) }
    }

    protected abstract suspend fun loadTopics(): List<TopicPresentable>

    protected abstract suspend fun loadDetailedTopicsInfo(
        topics: List<TopicPresentable>
    ): Pair<List<TopicPresentable>, Throwable?>

    protected abstract suspend fun getTopicConfig(topicName: String, showFullConfig: Boolean): List<TopicConfig>

    protected abstract suspend fun loadConsumerGroups(): List<ConsumerGroupPresentable>

    protected abstract suspend fun listConsumerGroupOffsets(consumerGroup: String): List<ConsumerGroupOffsetInfo>

    protected abstract suspend fun listSchemasNames(
        limit: Int?,
        filter: String?
    ): Pair<List<KafkaSchemaInfo>, Boolean>

    protected abstract suspend fun updateSchemaList(
        schemas: List<KafkaSchemaInfo>
    ): Pair<List<KafkaSchemaInfo>, Throwable?>

    protected abstract suspend fun listSchemaVersions(schemaName: String): List<Long>

    abstract suspend fun loadConsumerGroupOffset(name: String): List<ConsumerGroupOffsetInfo>

    abstract suspend fun loadTopicInfo(name: String): TopicPresentable

    abstract suspend fun resetOffsets(
        consumeGroupId: String,
        offsets: Map<TopicPartition, OffsetAndMetadata>
    )

    abstract suspend fun getOffsetsForData(
        partitions: Set<TopicPartition>,
        timestamp: Long
    ): Map<TopicPartition, Long>

    abstract suspend fun createTopic(
        name: String,
        partitions: Int?,
        replicationFactor: Int?,
        configs: Map<String, String> = emptyMap()
    ): Result<Unit>

    abstract suspend fun deleteTopic(topicNames: List<String>): Result<Unit>

    fun deleteTopicWithConfirmation(topicNames: List<String>) {
        if (topicNames.isEmpty()) return

        driver.coroutineScope.launch {
            try {
                val msg = if (topicNames.size == 1)
                    KafkaMessagesBundle.message("action.delete.topic.single.message", topicNames.first())
                else
                    KafkaMessagesBundle.message("action.delete.topic.multi.message", topicNames.size)

                val res = withContext(Dispatchers.EDT) {
                    Messages.showOkCancelDialog(
                        project,
                        msg,
                        KafkaMessagesBundle.message("action.delete.topic.title"),
                        CommonBundle.getOkButtonText(),
                        CommonBundle.getCancelButtonText(),
                        Messages.getQuestionIcon()
                    )
                }
                if (res != Messages.OK) return@launch

                val result = deleteTopic(topicNames)
                result.onFailure { exception ->
                    RfsNotificationUtils.showExceptionMessage(project, exception)
                }
            } catch (t: Throwable) {
                RfsNotificationUtils.showExceptionMessage(project, t)
            }
        }
    }

    abstract suspend fun clearTopic(topicName: String): Result<Unit>

    open fun clearPartitions(partitions: List<BdtTopicPartition>) {}

    open fun supportsClearPartitions(): Boolean = true

    open fun supportsInSyncReplicasData(): Boolean = true

    // Consumer panel feature capabilities

    abstract fun supportsConsumerGroups(): Boolean

    // Flag for filtering saved presets depending on CCloud ("ccloud") vs Native (null) connection type
    open fun presetConnectionTag(): String? = null

    open fun supportedConsumerProperties(): Set<String> = KafkaConsumerSettings.ALL_PROPERTIES

    val consumerPanelStorage: KafkaConsumerPanelStorage by lazy {
        KafkaConsumerPanelStorage(this).also { Disposer.register(this, it) }
    }

    fun getTopics(): List<TopicPresentable> = topicModel.data ?: emptyList()

    @RequiresBackgroundThread
    abstract fun loadTopicNames(): List<TopicPresentable>

    fun updatePinnedTopics(topicName: String, isForAdding: Boolean) {
        val config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(connectionId)
        if (isForAdding) {
            config.topicsPined += topicName
        } else {
            config.topicsPined -= topicName
        }

        driver.coroutineScope.launch(Dispatchers.Default) {
            val currentTopics = topicModel.data ?: emptyList()
            val updatedTopics = currentTopics.map { topic ->
                if (topic.name == topicName) {
                    topic.copy(isFavorite = isForAdding)
                } else {
                    topic
                }
            }.sortedWith(
                compareByDescending<TopicPresentable> { it.isFavorite }
                    .thenBy { it.name.lowercase() }
            )

            topicModel.setData(updatedTopics)
        }
    }

    fun getConsumerGroups(): List<ConsumerGroupPresentable> = consumerGroupsModel.data ?: emptyList()

    fun getCachedConsumerGroup(consumerGroup: String): ConsumerGroupPresentable? =
        consumerGroupsModel.data?.firstOrNull { it.consumerGroup == consumerGroup }

    fun updatePinnedConsumerGroups(consumerGroup: String, isForAdding: Boolean) {
        val config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(connectionId)
        if (isForAdding) {
            config.consumerGroupPined += consumerGroup
        } else {
            config.consumerGroupPined -= consumerGroup
        }

        driver.coroutineScope.launch(Dispatchers.Default) {
            val currentGroups = consumerGroupsModel.data ?: emptyList()
            val updatedGroups = currentGroups.map { group ->
                if (group.consumerGroup == consumerGroup) {
                    group.copy(isFavorite = isForAdding)
                } else {
                    group
                }
            }.sortedWith(
                compareByDescending<ConsumerGroupPresentable> { it.isFavorite }
                    .thenBy { it.consumerGroup.lowercase() }
            )

            consumerGroupsModel.setData(updatedGroups)
        }
    }

    fun getSchemas(): List<KafkaSchemaInfo> = schemaRegistryModel?.data ?: emptyList()

    fun getCachedSchema(name: String): KafkaSchemaInfo? =
        schemaRegistryModel?.data?.firstOrNull { it.name == name }

    fun getSchemaByName(name: String) = getCachedSchema(name)

    fun schemaExists(name: String) = getCachedSchema(name) != null

    open fun initRefreshSchemasIfRequired() {
        val schemaModel = schemaRegistryModel
        if (schemaModel?.isInitedByFirstTime == false) {
            updater.invokeRefreshModel(schemaModel)
        }
    }

    /**
     * Names of the schemas the editors can reference, without their formats.
     *
     * The editors, and their key and value fields, ask for the names all at the same time, so they share one
     * registry request, and what it found is reused for [SCHEMA_NAMES_TTL_MS]. A listing of a big registry takes
     * seconds, so what is older is still there for the editors to show, see [getCachedSchemaNames].
     * `null` if the registry could not be asked.
     */
    override suspend fun getSchemaNamesForEditor(): List<String>? = try {
        schemaNames.get()
    } catch (e: ProcessCanceledException) {
        throw e
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        thisLogger().warn("Failed to list schema names for connection $connectionId", e)
        null
    }

    override fun getCachedSchemaNames(): List<String>? = schemaNames.peek()

    override fun getKnownSchemaFormat(name: String): KafkaRegistryFormat? = schemaFormats.known(name)

    override suspend fun getSchemaFormat(name: String): KafkaRegistryFormat? = schemaFormats.formatOf(name)

    override suspend fun resolveSchemaFormats(
        names: Collection<String>,
        first: Collection<String>,
        onResolved: (name: String, format: KafkaRegistryFormat) -> Unit
    ) = schemaFormats.resolveAll(names, first, onResolved)

    /** Loads the details of the schema [name] from the registry, never from what is cached. */
    protected abstract suspend fun loadSchemaDetails(name: String): KafkaSchemaInfo

    /** The schemas of the registry changed: the editors must not keep offering the names they got before. */
    protected fun invalidateSchemaNames() {
        schemaNames.invalidate()
    }

    abstract suspend fun getLatestVersionInfo(schemaName: String): SchemaVersionInfo?

    abstract suspend fun getCachedOrLoadSchema(name: String): KafkaSchemaInfo

    abstract fun getSchemaVersionInfo(schemaName: String, version: Long): Promise<SchemaVersionInfo>

    abstract fun parseSchemaForDisplay(versionInfo: SchemaVersionInfo): Result<io.confluent.kafka.schemaregistry.ParsedSchema>

    // Schema write operations - override in subclasses that support writes (KafkaDataManager)
    open fun updateSchema(versionInfo: SchemaVersionInfo, newSchema: String): Promise<Unit> {
        throw UnsupportedOperationException("Schema updates not supported for this connection type")
    }

    open fun deleteRegistrySchemaVersion(versionInfo: SchemaVersionInfo) {
        throw UnsupportedOperationException("Schema deletion not supported for this connection type")
    }

    fun getSchemaVersionsModel(schemaName: String) = schemaVersionModels[schemaName]

    open fun updatePinnedSchemas(schemaName: String, isForAdding: Boolean) {
        val config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(getSchemaRegistryConfigId())
        if (isForAdding) {
            config.schemasPined += schemaName
        } else {
            config.schemasPined -= schemaName
        }

        driver.coroutineScope.launch(Dispatchers.Default) {
            schemaRegistryModel?.let { model ->
                val currentSchemas = model.data ?: emptyList()
                val updatedSchemas = currentSchemas.map { schema ->
                    if (schema.name == schemaName) {
                        schema.copy(isFavorite = isForAdding)
                    } else {
                        schema
                    }
                }.sortedWith(
                    compareByDescending<KafkaSchemaInfo> { it.isFavorite }
                        .thenBy { it.name.lowercase() }
                )

                model.setData(updatedSchemas)
            }
        }
    }

    protected fun applyTopicFilters(
        topics: List<TopicPresentable>,
        showInternalTopics: Boolean,
        filterName: String?
    ) = ClusterDataFilters.applyTopicFilters(topics, showInternalTopics, filterName)

    protected fun sortTopicsWithFavorites(
        topics: List<TopicPresentable>,
        pinnedTopics: Set<String>,
        showFavoriteOnly: Boolean
    ) = ClusterDataFilters.sortTopicsWithFavorites(topics, pinnedTopics, showFavoriteOnly)

    protected fun applyTopicLimit(
        topics: List<TopicPresentable>,
        limit: Int?
    ) = ClusterDataFilters.applyTopicLimit(topics, limit)

    protected fun applySchemaLimit(
        schemas: List<KafkaSchemaInfo>,
        limit: Int?
    ): Pair<List<KafkaSchemaInfo>, Boolean> {
        return if (limit != null && schemas.size > limit) {
            schemas.take(limit) to true
        } else {
            schemas to false
        }
    }

    protected fun applyConsumerGroupFilters(
        groups: List<ConsumerGroupPresentable>,
        filterName: String?
    ) = ClusterDataFilters.applyConsumerGroupFilters(groups, filterName)

    protected fun applyConsumerGroupLimit(
        groups: List<ConsumerGroupPresentable>,
        limit: Int?
    ) = ClusterDataFilters.applyConsumerGroupLimit(groups, limit)

    protected fun applySchemaFilters(
        schemas: List<KafkaSchemaInfo>,
        filterName: String?
    ) = ClusterDataFilters.applySchemaFilters(schemas, filterName)

    protected fun sortSchemasWithFavorites(
        schemas: List<KafkaSchemaInfo>,
        pinnedSchemas: Set<String>,
        showFavoriteOnly: Boolean
    ) = ClusterDataFilters.sortSchemasWithFavorites(schemas, pinnedSchemas, showFavoriteOnly)

    private fun createTopicsDataModel() = ObjectDataModel(
        idFieldName = TopicPresentable::name,
        additionalInfoLoading = { model ->
            val basicTopics = model.data ?: emptyList()
            catchFailures(onFailure = { t ->
                thisLogger().warn("Failed to load detailed topics info for connection $connectionId", t)
                basicTopics to t
            }) {
                runBlockingMaybeCancellable {
                    loadDetailedTopicsInfo(basicTopics)
                }
            }
        }
    ) {
        val toolWindowSettings = KafkaToolWindowSettings.getInstance()
        val config = toolWindowSettings.getOrCreateConfig(connectionId)

        val rawTopics = runBlockingMaybeCancellable {
            loadTopics()
        }

        val filteredTopics = applyTopicFilters(
            rawTopics,
            showInternalTopics = toolWindowSettings.showInternalTopics,
            filterName = config.topicFilterName
        )

        val sortedTopics = sortTopicsWithFavorites(
            filteredTopics,
            pinnedTopics = config.topicsPined,
            showFavoriteOnly = toolWindowSettings.showFavoriteTopics
        )

        applyTopicLimit(sortedTopics, config.topicLimit)
    }

    protected open fun createTopicPartitionsStorage() = ObjectDataModelStorage<String, BdtTopicPartition>(
        updater,
        BdtTopicPartition::partitionId,
        dependOn = topicModel
    ) { topicName ->
        val topics = topicModel.data ?: emptyList()
        val topic = topics.find { it.name == topicName }
            ?: error(KafkaMessagesBundle.message("topic.not.found", topicName))
        topic.partitionList
    }

    private fun createTopicConfigsStorage() = ObjectDataModelStorage<String, TopicConfig>(
        updater,
        TopicConfig::name
    ) { topicName ->
        try {
            val showFullConfig = KafkaToolWindowSettings.getInstance().showFullTopicConfig
            runBlockingMaybeCancellable {
                getTopicConfig(topicName, showFullConfig)
            }
        } catch (e: Exception) {
            thisLogger().warn("Failed to load configs for topic '$topicName'", e)
            emptyList()
        }
    }

    private fun createConsumerGroupsDataModel() = ObjectDataModel(
        idFieldName = ConsumerGroupPresentable::consumerGroup
    ) {
        val toolWindowSettings = KafkaToolWindowSettings.getInstance()
        val config = toolWindowSettings.getOrCreateConfig(connectionId)

        val rawGroups = runBlockingMaybeCancellable {
            loadConsumerGroups()
        }

        val filteredGroups = applyConsumerGroupFilters(
            rawGroups,
            filterName = config.consumerFilterName
        )

        applyConsumerGroupLimit(filteredGroups, config.consumerLimit)
    }

    private fun createConsumerGroupOffsetsStorage() = ObjectDataModelStorage<String, ConsumerGroupOffsetInfo>(
        updater,
        ConsumerGroupOffsetInfo::fullId
    ) { consumerGroup ->
        try {
            runBlockingMaybeCancellable {
                listConsumerGroupOffsets(consumerGroup)
            }
        } catch (e: Exception) {
            thisLogger().warn("Failed to load offsets for consumer group '$consumerGroup'", e)
            emptyList()
        }
    }

    protected open fun createSchemaRegistryDataModel(): ObjectDataModel<KafkaSchemaInfo>? {
        if (registryType == KafkaRegistryType.NONE) return null

        return ObjectDataModel(
            idFieldName = KafkaSchemaInfo::name,
            additionalInfoLoading = { model ->
                val basicSchemas = model.data ?: emptyList()
                catchFailures(onFailure = { t ->
                    thisLogger().warn("Failed to update schema list for connection $connectionId", t)
                    basicSchemas.map { it.copy(type = null, version = null) } to t
                }) {
                    runBlockingMaybeCancellable {
                        updateSchemaList(basicSchemas)
                    }
                }
            }
        ) {
            val toolWindowSettings = KafkaToolWindowSettings.getInstance()
            val config = toolWindowSettings.getOrCreateConfig(connectionId)

            val (rawSchemas, _) = runBlockingMaybeCancellable {
                listSchemasNames(
                    limit = null, // Don't apply limit here, apply after sorting (same as topics)
                    filter = config.schemaFilterName
                )
            }

            val sortedSchemas = sortSchemasWithFavorites(
                rawSchemas,
                pinnedSchemas = config.schemasPined,
                showFavoriteOnly = toolWindowSettings.showFavoriteSchema
            )

            applySchemaLimit(sortedSchemas, config.registryLimit)
        }
    }

    private fun createSchemaVersionsStorage() = FieldGroupsDataModelStorage<String, List<Long>>(updater) { schemaName ->
        try {
            val versions = runBlockingMaybeCancellable {
                listSchemaVersions(schemaName)
            }
            FieldGroupsData(versions.sortedDescending(), emptyList())
        } catch (e: Exception) {
            thisLogger().warn("Failed to load versions for schema '$schemaName'", e)
            FieldGroupsData(emptyList(), emptyList())
        }
    }

    private companion object {
        // How long the names of all the schemas are kept. The editors show older ones while the registry is asked again.
        const val SCHEMA_NAMES_TTL_MS = 60_000L
    }
}
