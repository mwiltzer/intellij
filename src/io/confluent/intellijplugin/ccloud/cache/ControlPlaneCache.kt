package io.confluent.intellijplugin.ccloud.cache

import com.intellij.openapi.progress.runBlockingMaybeCancellable
import com.intellij.openapi.project.Project
import io.confluent.intellijplugin.ccloud.client.CCloudRestClient
import io.confluent.intellijplugin.ccloud.config.CloudConfig
import io.confluent.intellijplugin.ccloud.fetcher.ControlPlaneFetcher
import io.confluent.intellijplugin.ccloud.fetcher.ControlPlaneFetcherImpl
import io.confluent.intellijplugin.ccloud.model.Cluster
import io.confluent.intellijplugin.ccloud.model.Environment
import io.confluent.intellijplugin.ccloud.model.SchemaRegistry
import io.confluent.intellijplugin.core.monitoring.connection.MonitoringClient
import org.jetbrains.annotations.VisibleForTesting
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

/**
 * Control plane cache for organizational resources (environments, clusters, schema registries).
 * Caches data from ControlPlaneFetcherImpl to reduce API calls. Use refresh*() methods to update cache.
 * Safe to use from several threads: tree nodes, background refreshes and the editors all read it.
 */
class ControlPlaneCache @VisibleForTesting internal constructor(
    project: Project?,
    fetcher: ControlPlaneFetcher?
) : MonitoringClient(project) {

    /** Primary constructor for production use. */
    constructor(project: Project?) : this(project, null)

    @Volatile
    private var fetcher: ControlPlaneFetcher? = fetcher

    // Cached data
    @Volatile
    private var cachedEnvironments: List<Environment>? = null
    private val cachedClusters = ConcurrentHashMap<String, List<Cluster>>()

    // An empty Optional records that the environment has no Schema Registry, so it is not asked for again.
    private val cachedSchemaRegistry = ConcurrentHashMap<String, Optional<SchemaRegistry>>()

    override fun getRealUri(): String = CloudConfig.CONTROL_PLANE_BASE_URL

    override fun connectInner(calledByUser: Boolean) {
        val client = CCloudRestClient(
            baseUrl = CloudConfig.CONTROL_PLANE_BASE_URL,
            authType = CCloudRestClient.AuthType.CONTROL_PLANE
        )
        fetcher = ControlPlaneFetcherImpl(client)
    }

    override fun checkConnectionInner() {
        val f = fetcher ?: throw IllegalStateException("Cache not initialized")
        // Validate connection by fetching environments
        cachedEnvironments = runBlockingMaybeCancellable {
            f.getEnvironments()
        }
    }

    /** Get cached environments (empty if not loaded). */
    fun getEnvironments(): List<Environment> = cachedEnvironments ?: emptyList()

    /** Get clusters for an environment (fetches on first access, then cached). */
    fun getKafkaClusters(environmentId: String): List<Cluster> {
        cachedClusters[environmentId]?.let { return it }
        val f = fetcher ?: return emptyList()
        val clusters = runBlockingMaybeCancellable { f.getKafkaClusters(environmentId) }
        // A refresh may have finished while fetching; its (newer) result wins.
        return cachedClusters.putIfAbsent(environmentId, clusters) ?: clusters
    }

    fun getCachedKafkaClusters(environmentId: String): List<Cluster>? = cachedClusters[environmentId]

    /**
     * Get schema registry for an environment (fetches on first access, then cached, also when the environment
     * has none). Returns null if none exists.
     */
    fun getSchemaRegistry(environmentId: String): SchemaRegistry? {
        cachedSchemaRegistry[environmentId]?.let { return it.orElse(null) }
        val f = fetcher ?: return null
        val fetched = Optional.ofNullable(runBlockingMaybeCancellable { f.getSchemaRegistry(environmentId) })
        return (cachedSchemaRegistry.putIfAbsent(environmentId, fetched) ?: fetched).orElse(null)
    }

    fun getCachedSchemaRegistry(environmentId: String): SchemaRegistry? =
        cachedSchemaRegistry[environmentId]?.orElse(null)

    suspend fun refreshEnvironments(): List<Environment> {
        cachedEnvironments = fetcher?.let { f ->
            f.getEnvironments()
        } ?: emptyList()
        return cachedEnvironments ?: emptyList()
    }

    suspend fun refreshKafkaClusters(environmentId: String): List<Cluster> {
        val clusters = fetcher?.let { f ->
            f.getKafkaClusters(environmentId)
        } ?: emptyList()
        cachedClusters[environmentId] = clusters
        return clusters
    }

    suspend fun refreshSchemaRegistry(environmentId: String): SchemaRegistry? {
        // Without a fetcher nothing was asked, so nothing may be cached as "no schema registry".
        val f = fetcher ?: return null
        val registry = f.getSchemaRegistry(environmentId)
        cachedSchemaRegistry[environmentId] = Optional.ofNullable(registry)
        return registry
    }

    fun clearCache() {
        cachedEnvironments = null
        cachedClusters.clear()
        cachedSchemaRegistry.clear()
    }

    override fun dispose() {
        fetcher = null
        clearCache()
    }
}

