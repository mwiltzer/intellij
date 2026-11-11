package io.confluent.intellijplugin.data

import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.progress.ProcessCanceledException
import io.confluent.intellijplugin.core.monitoring.data.model.ObjectDataModel
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.registry.common.hasLoadedDetails
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean

/**
 * What the registry listed: at least the names, in the order of the names regardless of the case. Listing a registry
 * with thousands of schemas takes seconds, so one listing is shared by the tool window and the editors.
 */
internal class SchemaListing(val schemas: List<KafkaSchemaInfo>) {
    val names: List<String> = schemas.map { it.name }
    private val nameSet: Set<String> by lazy(LazyThreadSafetyMode.PUBLICATION) { names.toHashSet() }

    operator fun contains(name: String): Boolean = name in nameSet
}

/**
 * Keeps the schema list of the tool window in step with the settings that shape it (filter, limit, favorites, ...)
 * without asking the registry again: it is made out of the [SchemaListing] that was loaded last. A change of the
 * settings is shown at once, then the details of the schemas that are new in the list are loaded, a few at a time.
 *
 * The details are merged into the list that the model has when they arrive, so late details never bring back
 * schemas of an earlier filter.
 *
 * @param cachedListing the listing that was loaded last, or `null` if there is none
 * @param refreshFromRegistry makes the model load the list from the registry, for when there is no listing
 */
internal class SchemaRegistryList(
    private val scope: CoroutineScope,
    private val resolver: SchemaFormatResolver,
    private val cachedListing: () -> SchemaListing?,
    private val settings: () -> SchemaListSettings,
    private val model: () -> ObjectDataModel<KafkaSchemaInfo>?,
    private val refreshFromRegistry: () -> Unit,
    private val detailsDelayMillis: Long = DETAILS_DELAY_MS
) {
    // Guards the changes of the model, and the details job.
    private val lock = Any()
    private var detailsJob: Job? = null

    // Set while a change of the settings waits to be applied: that one will see the latest settings.
    private val applyQueued = AtomicBoolean(false)

    /** The list to show for the [listing] the registry gave, and whether the limit left out more. */
    fun view(listing: SchemaListing): Pair<List<KafkaSchemaInfo>, Boolean> {
        val view = ClusterDataFilters.schemaListView(listing.schemas, settings())
        return withKnownDetails(view.schemas, model()?.data) to view.hasMore
    }

    /**
     * Shows the listing that was loaded last with the settings as they are now. Cheap, so it can follow every key
     * typed into the filter. Changes that come in at the same time are applied together.
     */
    fun refilter() {
        if (!applyQueued.compareAndSet(false, true)) return

        scope.launch {
            applyQueued.set(false)
            apply()
        }
    }

    /**
     * Merges the [loaded] details into what the [model] has now, whatever that is by then, and returns that.
     * For details that were loaded for the list the model had earlier.
     */
    fun mergeLoaded(model: ObjectDataModel<KafkaSchemaInfo>, loaded: List<KafkaSchemaInfo>): List<KafkaSchemaInfo> {
        synchronized(lock) {
            val current = model.data ?: return loaded
            return mergeDetails(current, loaded).also { model.setData(it) }
        }
    }

    private fun apply() {
        val shown = show(cachedListing(), settings())

        if (shown == null) {
            // Nothing was listed yet, so there is nothing to show but what the registry says.
            if (model() != null) refreshFromRegistry()
            return
        }

        loadDetails(shown)
    }

    /** Shows the [listing] with the [settings], and returns the list. `null` if there is nothing to show it with or in. */
    private fun show(listing: SchemaListing?, settings: SchemaListSettings): List<KafkaSchemaInfo>? =
        synchronized(lock) {
            val model = model()
            if (listing == null || model == null) {
                null
            } else {
                val view = ClusterDataFilters.schemaListView(listing.schemas, settings)
                withKnownDetails(view.schemas, model.data).also { model.setData(it, view.hasMore) }
            }
        }

    private fun loadDetails(shown: List<KafkaSchemaInfo>) {
        val missing = shown.filter { !it.hasLoadedDetails() }.map { it.name }

        synchronized(lock) {
            detailsJob?.cancel()
            detailsJob = if (missing.isEmpty()) null else scope.launch {
                try {
                    // The next key of a filter that is typed is likely to change what is shown.
                    delay(detailsDelayMillis)
                    val loaded = resolver.detailsOf(missing)
                    mergeIntoModel(loaded.values)
                } catch (e: ProcessCanceledException) {
                    throw e
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    thisLogger().warn("Failed to load the details of ${missing.size} schemas", e)
                }
            }
        }
    }

    private fun mergeIntoModel(loaded: Collection<KafkaSchemaInfo>) {
        synchronized(lock) {
            val model = model() ?: return
            val current = model.data ?: return
            model.setData(mergeDetails(current, loaded))
        }
    }

    /** The details that are known for the [schemas] already, from the list that is shown or from the registry. */
    private fun withKnownDetails(schemas: List<KafkaSchemaInfo>, shownNow: List<KafkaSchemaInfo>?): List<KafkaSchemaInfo> {
        val shownByName = shownNow?.filter { it.hasLoadedDetails() }?.associateBy { it.name } ?: emptyMap()
        return schemas.map { schema ->
            if (schema.hasLoadedDetails()) schema
            else (shownByName[schema.name] ?: resolver.knownInfo(schema.name))
                ?.copy(isFavorite = schema.isFavorite) ?: schema
        }
    }

    companion object {
        // Longer than the time between the keys of somebody typing, and short next to loading the details.
        const val DETAILS_DELAY_MS = 300L

        /** The [current] list with the details of the [loaded] schemas that are in it. The others are left out. */
        fun mergeDetails(current: List<KafkaSchemaInfo>, loaded: Collection<KafkaSchemaInfo>): List<KafkaSchemaInfo> {
            if (loaded.isEmpty()) return current
            val loadedByName = loaded.associateBy { it.name }
            return current.map { schema -> loadedByName[schema.name]?.copy(isFavorite = schema.isFavorite) ?: schema }
        }
    }
}
