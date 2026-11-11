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
 *
 * It lists all the schemas of the registry, or only those that start with a [prefix], which is much cheaper.
 *
 * @param prefix what all the schemas of the listing start with, or `null` if it has every schema of the registry
 * @param loadedAt when the registry was asked
 */
internal class SchemaListing(
    val schemas: List<KafkaSchemaInfo>,
    val prefix: String? = null,
    val loadedAt: Long = System.currentTimeMillis()
) {
    val names: List<String> = schemas.map { it.name }
    private val nameSet: Set<String> by lazy(LazyThreadSafetyMode.PUBLICATION) { names.toHashSet() }

    operator fun contains(name: String): Boolean = name in nameSet

    /**
     * Whether the listing has all the schemas a filter that asks for [filterPrefix] lets through, see
     * [ClusterDataFilters.prefixOf]. `null` is a filter that is no prefix, which only a listing of all schemas serves.
     */
    fun covers(filterPrefix: String?): Boolean =
        prefix == null || (filterPrefix != null && filterPrefix.startsWith(prefix))

    /** Whether the listing tells if the registry has the schema [name], as of [loadedAt]. */
    fun knowsAbout(name: String): Boolean = prefix == null || name.startsWith(prefix)
}

/** Picks among the listings that were loaded, which each serve some filters and some schemas. */
internal object SchemaListings {

    /**
     * The listing to make the list out of for a filter that asks for [prefix] (`null` if it is no prefix): of those of
     * the [candidates] that have all the schemas the filter lets through the one that was loaded last. Of two that were
     * loaded at the same time the more general one, which serves more filters: the one with all the schemas of the
     * registry, or else the one for the shorter prefix.
     */
    fun bestFor(prefix: String?, candidates: Collection<SchemaListing>): SchemaListing? =
        candidates.filter { it.covers(prefix) }.maxWithOrNull(RECENT_AND_GENERAL)

    /** The listing that was loaded last of those of the [candidates] that tell if the registry has the schema [name]. */
    fun knowingAbout(name: String, candidates: Collection<SchemaListing>): SchemaListing? =
        candidates.filter { it.knowsAbout(name) }.maxWithOrNull(RECENT_AND_GENERAL)

    // A listing of all schemas is more general than one for a prefix, and the shorter the prefix, the more general.
    private fun generality(listing: SchemaListing): Int = listing.prefix?.let { -it.length } ?: 1

    private val RECENT_AND_GENERAL = compareBy<SchemaListing>({ it.loadedAt }, { generality(it) })
}

/** The names of the topics that the user can see, or `null` if they could not be listed. */
internal class VisibleTopics(val names: Set<String>?)

/**
 * Keeps the schema list of the tool window in step with the settings that shape it (filter, limit, favorites, ...)
 * without asking the registry again: it is made out of the [SchemaListing] that was loaded last. A change of the
 * settings is shown at once, then the details of the schemas that are new in the list are loaded, a few at a time.
 *
 * The details are merged into the list that the model has when they arrive, so late details never bring back
 * schemas of an earlier filter.
 *
 * A filter that starts with `^` asks for the schemas that start with what follows, see [ClusterDataFilters.prefixOf].
 * A registry lists those a lot faster than all schemas, so if no listing has them yet they are fetched, once the
 * filter has not changed for [prefixSettleMillis]. The one that is fetched serves the longer prefixes that come
 * next, which is what typing does, without asking the registry again.
 *
 * @param cachedListing the listing that was loaded last and has all the schemas that a filter for the given prefix lets
 * through (`null` for a filter that is no prefix, which needs all schemas), or `null` if there is none
 * @param refreshFromRegistry makes the model load the list from the registry, for when there is no listing
 * @param prepare gets what the [settings] need before a change of them is applied, e.g. the topics that are visible
 * @param fetchListing lists the registry for the schemas with the given prefix; `null` if that can not be done, which
 * makes the model load the list
 */
internal class SchemaRegistryList(
    private val scope: CoroutineScope,
    private val resolver: SchemaFormatResolver,
    private val cachedListing: (prefix: String?) -> SchemaListing?,
    private val settings: () -> SchemaListSettings,
    private val model: () -> ObjectDataModel<KafkaSchemaInfo>?,
    private val refreshFromRegistry: () -> Unit,
    private val detailsDelayMillis: Long = DETAILS_DELAY_MS,
    private val prepare: suspend () -> Unit = {},
    private val fetchListing: suspend (prefix: String) -> SchemaListing? = { null },
    private val prefixSettleMillis: Long = PREFIX_SETTLE_MS
) {
    // Guards the changes of the model, and the details and the prefix jobs.
    private val lock = Any()
    private var detailsJob: Job? = null
    private var prefixJob: Job? = null

    // Set while a change of the settings waits to be applied: that one will see the latest settings.
    private val applyQueued = AtomicBoolean(false)

    /** What the settings left out of the list that was shown last. It is up to date when the model tells of a change. */
    @Volatile
    var hidden: HiddenSchemas = HiddenSchemas.NONE
        private set

    /**
     * The list to show for the [listing] the registry gave, and whether the limit left out more. The filter may have
     * changed while the registry was listed. Then a listing that serves it better is used, if there is one.
     */
    fun view(listing: SchemaListing): Pair<List<KafkaSchemaInfo>, Boolean> {
        val settings = settings()
        val prefix = ClusterDataFilters.prefixOf(settings.filter)
        val usable = listing.takeIf { it.covers(prefix) } ?: cachedListing(prefix) ?: listing
        val view = ClusterDataFilters.schemaListView(usable.schemas, settings)
        hidden = view.hidden
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
            try {
                prepare()
            } catch (e: ProcessCanceledException) {
                throw e
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Not knowing what the settings need must not keep the list from following them.
                thisLogger().warn("Failed to prepare the schema list for its settings", e)
            }
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
        val settings = settings()
        val prefix = ClusterDataFilters.prefixOf(settings.filter)
        val shown = show(cachedListing(prefix), settings)

        if (shown == null) {
            onMissingListing(prefix)
            return
        }

        // What the filter asks for is shown, so what was to be fetched for an earlier filter is not needed.
        cancelPrefixFetch()
        loadDetails(shown)
    }

    /**
     * Shows the [listing] with the [settings], and returns the list. `null` if there is nothing to show it with or in:
     * no listing, no model, or a listing that has not all the schemas the filter lets through.
     */
    private fun show(listing: SchemaListing?, settings: SchemaListSettings): List<KafkaSchemaInfo>? =
        synchronized(lock) {
            val model = model()
            if (listing == null || model == null || !listing.covers(ClusterDataFilters.prefixOf(settings.filter))) {
                null
            } else {
                val view = ClusterDataFilters.schemaListView(listing.schemas, settings)
                hidden = view.hidden
                withKnownDetails(view.schemas, model.data).also { model.setData(it, view.hasMore) }
            }
        }

    // No listing has what the filter asks for.
    private fun onMissingListing(prefix: String?) {
        when {
            model() == null -> Unit
            // Nothing was listed yet, so there is nothing to show but what the registry says.
            prefix == null -> refreshFromRegistry()
            // A caret alone is a filter that is not typed yet. Listing all schemas for it would be a waste, so the list
            // stays as it is, until there is a prefix.
            prefix.isEmpty() -> Unit
            else -> fetchLater()
        }
    }

    private fun fetchLater() {
        synchronized(lock) {
            prefixJob?.cancel()
            prefixJob = scope.launch {
                // The next key of a filter that is typed is likely to change what is asked for.
                delay(prefixSettleMillis)
                fetchAndShow()
            }
        }
    }

    private fun cancelPrefixFetch() {
        synchronized(lock) {
            prefixJob?.cancel()
            prefixJob = null
        }
    }

    // For the filter as it is now, which may be another than the one that made the fetch wait.
    private suspend fun fetchAndShow() {
        val prefix = ClusterDataFilters.prefixOf(settings().filter)?.takeIf { it.isNotEmpty() } ?: return
        try {
            // A fetch that was started for an earlier prefix may have brought it meanwhile.
            val listing = cachedListing(prefix) ?: fetchListing(prefix)
            if (listing == null) {
                refreshFromRegistry()
                return
            }
            // The filter may have changed while the registry was asked. What counts is the filter that is there now: the
            // listing is shown for it if it has what that asks for, else the change of the filter shows its own list.
            show(listing, settings())?.let { loadDetails(it) }
        } catch (e: ProcessCanceledException) {
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // The model lists the registry anew, and tells what is wrong with it if it fails as well.
            thisLogger().warn("Failed to list the schemas that start with '$prefix'", e)
            refreshFromRegistry()
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

        // Typing a prefix changes it with every key. The registry is asked when the typing pauses.
        const val PREFIX_SETTLE_MS = 300L

        /** The [current] list with the details of the [loaded] schemas that are in it. The others are left out. */
        fun mergeDetails(current: List<KafkaSchemaInfo>, loaded: Collection<KafkaSchemaInfo>): List<KafkaSchemaInfo> {
            if (loaded.isEmpty()) return current
            val loadedByName = loaded.associateBy { it.name }
            return current.map { schema -> loadedByName[schema.name]?.copy(isFavorite = schema.isFavorite) ?: schema }
        }
    }
}
