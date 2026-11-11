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
import io.confluent.intellijplugin.registry.SchemaUsage
import io.confluent.intellijplugin.registry.SchemaVersionInfo
import io.confluent.intellijplugin.toolwindow.config.KafkaToolWindowSettings
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import io.confluent.intellijplugin.util.catchFailures
import io.confluent.intellijplugin.util.timed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.jetbrains.concurrency.Promise

abstract class BaseClusterDataManager(
    project: Project?,
    settings: IntervalUpdateSettings,
    driverProvider: () -> MonitoringDriver
) : MonitoringDataManager(project, settings, driverProvider), SchemaFormats, SchemaUsageSource {

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

    // What the registry lists, shared by the tool window and the editors. Changes through the plugin and refreshes of
    // the tool window invalidate it sooner than it expires, and what is older is still shown by the editors while the
    // registry is asked again, see getCachedSchemaNames.
    private val schemaListing: ExpiringSingleFlight<SchemaListing> by lazy {
        schemaNamesFlight(schemaScope, prefixListings) {
            thisLogger().timed("Listing the schema names of connection $connectionId") {
                SchemaListing(listSchemasNames().sortedBy { it.name.lowercase() })
            }
        }
    }
    // What the registry lists for a prefix: the filter of the tool window may ask for the schemas that start with a
    // text, and a registry with thousands of schemas lists those a lot faster than all of them.
    private val prefixListings: PrefixListings by lazy {
        PrefixListings(schemaScope) { prefix ->
            thisLogger().timed("Listing the schema names of connection $connectionId that start with '$prefix'") {
                listSchemasNamesByPrefix(prefix)?.let { SchemaListing(it.sortedBy { schema -> schema.name.lowercase() }, prefix) }
            }
        }
    }

    // Decides which listing serves what, and when the registry is asked.
    private val listings: SchemaListingCache by lazy { SchemaListingCache(schemaListing, prefixListings) }

    // What the records tell about their schemas, shared by all the editors of the connection.
    private val schemaUsages: RecordSchemaLookup by lazy {
        RecordSchemaLookup(schemaScope) { loadSchemaUsages(it) }
    }

    // The topics the user can see, to hide the schemas of the others. Not knowing them hides nothing.
    private val visibleTopics: ExpiringSingleFlight<VisibleTopics> by lazy {
        ExpiringSingleFlight(schemaScope, VISIBLE_TOPICS_TTL_MS) {
            catchFailures(onFailure = { t ->
                thisLogger().warn("Failed to list the topics of connection $connectionId for the schemas", t)
                VisibleTopics(null)
            }) {
                thisLogger().timed("Listing the topics of connection $connectionId for the schemas") {
                    VisibleTopics(listVisibleTopicNames())
                }
            }
        }
    }

    // The list of the tool window, which follows its settings without listing the registry again.
    private val schemaList: SchemaRegistryList by lazy {
        SchemaRegistryList(
            scope = schemaScope,
            resolver = schemaFormats,
            cachedListing = listings::cachedFor,
            fetchListing = { prefix -> listings.listingFor(prefix, refresh = false) },
            settings = ::currentSchemaListSettings,
            model = { schemaRegistryModel },
            refreshFromRegistry = { schemaRegistryModel?.let { updater.invokeRefreshModel(it) } },
            prepare = { if (hidesSchemasOfOtherTopics()) visibleTopics.get() }
        )
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

    /**
     * All the schemas the registry lists, as far as its listing tells about them: at least the names. The filter,
     * limit and favorites of the tool window are not applied here, see [SchemaRegistryList], so what is listed does
     * not depend on them.
     */
    protected abstract suspend fun listSchemasNames(): List<KafkaSchemaInfo>

    /**
     * The schemas whose names start with [prefix], as [listSchemasNames] lists them, for a registry that can leave out
     * all the others when it lists. `null` if it can not, which is then not tried again: all schemas are listed.
     */
    protected open suspend fun listSchemasNamesByPrefix(prefix: String): List<KafkaSchemaInfo>? = null

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

    /** How many schemas the settings keep out of the tool window, as of the list that is shown there. */
    internal val hiddenSchemas: HiddenSchemas get() = schemaList.hidden

    fun getCachedSchema(name: String): KafkaSchemaInfo? =
        schemaRegistryModel?.data?.firstOrNull { it.name == name }

    fun getSchemaByName(name: String) = getCachedSchema(name)

    /**
     * Whether the registry has the schema [name], as far as that is known without asking it. Also true for schemas
     * that the list of the tool window leaves out, because of its filter, its limit, or the schemas it hides.
     *
     * `null` if that is not known, e.g. because the list only has the schemas of a prefix and the registry was not asked
     * for the others yet: [checkSchemaExists] asks.
     */
    fun schemaExists(name: String): Boolean? =
        listings.knownToHave(name) ?: true.takeIf { getCachedSchema(name) != null }

    /** Whether the registry has the schema [name], found out with the cheapest request there is if it is not known. */
    suspend fun checkSchemaExists(name: String): Boolean = schemaExists(name) ?: listings.has(name)

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
     * registry request, and what it found is reused for [SCHEMA_NAMES_TTL_MS] or [SCHEMA_NAMES_SHORT_TTL_MS]. A listing
     * of a big registry takes seconds, so what is older is still there for the editors to show, see
     * [getCachedSchemaNames]; the schemas that were registered since are the ones [getSchemaNamesForTopic] finds.
     * `null` if the registry could not be asked.
     */
    override suspend fun getSchemaNamesForEditor(): List<String>? = try {
        schemaListing.get().names
    } catch (e: ProcessCanceledException) {
        throw e
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        thisLogger().warn("Failed to list schema names for connection $connectionId", e)
        null
    }

    override fun getCachedSchemaNames(): List<String>? = schemaListing.peek()?.names

    override suspend fun getSchemaNamesForTopic(topic: String): List<String>? = try {
        listings.namesStartingWith(topic)
    } catch (e: ProcessCanceledException) {
        throw e
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        // The listing of all schemas is what the editors ask for next, and tells what is wrong with the registry.
        thisLogger().warn("Failed to list the schema names of topic '$topic' for connection $connectionId", e)
        null
    }

    override fun getPinnedSchemaNames(): Set<String> =
        KafkaToolWindowSettings.getInstance().getOrCreateConfig(getSchemaRegistryConfigId()).schemasPined.toSet()

    /**
     * The versions of the schema [schemaName] that the editors offer, the newest first. Costs one registry request.
     * `null` if the registry could not be asked.
     */
    suspend fun getSchemaVersionsForEditor(schemaName: String): List<Long>? = try {
        listSchemaVersions(schemaName).sortedDescending()
    } catch (e: ProcessCanceledException) {
        throw e
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        thisLogger().warn("Failed to list the versions of schema '$schemaName' for connection $connectionId", e)
        null
    }

    /**
     * The subjects, and the versions of them, that the schema with the ID [schemaId] is registered as. This is what
     * a record tells about the schema it was written with. The registry answers once for all the records that share
     * the schema, see [RecordSchemaLookup].
     */
    override suspend fun getSchemaUsages(schemaId: Int): List<SchemaUsage>? = schemaUsages.usagesOf(schemaId)

    override fun getCachedSchemaUsages(schemaId: Int): List<SchemaUsage>? = schemaUsages.known(schemaId)

    /**
     * Asks the registry for the subjects and versions of the schema with the ID [schemaId]. Nothing if the registry
     * does not know the ID; `null` if it can not be asked, or has no schema IDs, as the registry of AWS Glue.
     */
    protected open suspend fun loadSchemaUsages(schemaId: Int): List<SchemaUsage>? = null

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
        listings.clear()
        schemaUsages.clear()
    }

    /**
     * Shows the schemas of the tool window as the filter, limit and favorites say now. The registry is not asked for
     * them again, they come from the listing that was loaded last. A refresh of the model lists the registry anew.
     */
    fun refilterSchemas() = schemaList.refilter()

    private fun currentSchemaListSettings(): SchemaListSettings = SchemaListSettingsReader.read(
        toolWindow = KafkaToolWindowSettings.getInstance(),
        config = KafkaToolWindowSettings.getInstance().getOrCreateConfig(getSchemaRegistryConfigId()),
        registryType = registryType,
        visibleTopics = { visibleTopics.peek()?.names }
    )

    private fun hidesSchemasOfOtherTopics() =
        SchemaListSettingsReader.hidesSchemasOfOtherTopics(registryType, KafkaToolWindowSettings.getInstance())

    /**
     * The names of the topics the user can see, internal ones included, or `null` if that can not be told.
     * What the cluster lists for the user, so what the user is not allowed to describe is not in it.
     */
    protected open suspend fun listVisibleTopicNames(): Set<String>? = null

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

        // Pinned schemas come first, and with favorites only the list is made of them.
        refilterSchemas()
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
                    schemaList.mergeLoaded(model, basicSchemas.map { it.copy(type = null, version = null) }) to t
                }) {
                    val (detailed, error) = runBlockingMaybeCancellable {
                        updateSchemaList(basicSchemas)
                    }
                    // The filter may have changed while the details were loaded, so they go into the list as it is now.
                    schemaList.mergeLoaded(model, detailed) to error
                }
            }
        ) { model ->
            // The first load can use what the editors listed already, a refresh is the way to see what others changed
            // in the registry, so it lists again. The editors profit from that, too.
            val listing = runBlockingMaybeCancellable {
                loadSchemaListing(refresh = model.isInitedByFirstTime)
            }
            schemaList.view(listing)
        }
    }

    /**
     * What the registry lists, for the tool window. The topics the user can see are needed to hide the schemas of the
     * others, so they are asked at the same time, and the same way (again on a refresh).
     */
    private suspend fun loadSchemaListing(refresh: Boolean): SchemaListing = coroutineScope {
        val topics = if (hidesSchemasOfOtherTopics()) async {
            if (refresh) visibleTopics.refresh() else visibleTopics.get()
        } else null

        val prefix = ClusterDataFilters.prefixOf(currentSchemaFilter())
        val listing = listings.listingFor(prefix, refresh)
        topics?.await()
        listing
    }

    private fun currentSchemaFilter(): String? =
        KafkaToolWindowSettings.getInstance().getOrCreateConfig(getSchemaRegistryConfigId()).schemaFilterName

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
        // The topics of a cluster change rarely. A refresh of the schemas lists them again.
        const val VISIBLE_TOPICS_TTL_MS = 5 * 60_000L
    }
}
