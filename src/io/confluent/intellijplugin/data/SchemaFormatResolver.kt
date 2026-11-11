package io.confluent.intellijplugin.data

import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.progress.ProcessCanceledException
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.registry.common.hasLoadedDetails
import io.confluent.intellijplugin.util.timed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap

/**
 * Knows the format (Avro, Protobuf, JSON) of the registry schemas, which the editors and the tool window need.
 *
 * The format of a schema takes one registry request to find out, and a registry can have thousands of schemas.
 * So the resolver
 *  - remembers the formats it found, so a schema is requested only once,
 *  - remembers the schemas whose format it could not find (a subject that may not be read, one that is gone, a
 *    registry that did not answer) for [retryUnresolvedAfterMillis], so a bulk request does not ask for them again and
 *    again, which is what a list that is painted over and over would do,
 *  - lets everybody who asks for the same schema at the same time share one request, whether it is the tool window
 *    refreshing its details or an editor looking for the schema of its topic,
 *  - loads the schemas of a bulk request ([resolveAll], [refreshAll]) at most [parallelism] at a time,
 *    while a single schema that somebody waits for ([formatOf]) never queues behind them,
 *  - runs the requests in its own [scope], so a caller that gives up does not fail the callers sharing its request.
 *
 * [loadSchema] must return the schema with its loaded details, see [hasLoadedDetails].
 */
internal class SchemaFormatResolver(
    parallelism: Int = DEFAULT_PARALLELISM,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    private val retryUnresolvedAfterMillis: Long = DEFAULT_RETRY_UNRESOLVED_AFTER_MS,
    private val clock: () -> Long = System::currentTimeMillis,
    private val loadSchema: suspend (String) -> KafkaSchemaInfo
) {
    private val logger = thisLogger()
    private val formats = ConcurrentHashMap<String, KafkaRegistryFormat>()

    // When the format of a schema could not be found, by the clock.
    private val unresolved = ConcurrentHashMap<String, Long>()
    private val inFlight = ConcurrentHashMap<String, Deferred<KafkaSchemaInfo?>>()
    private val bulkSlots = Semaphore(parallelism)

    /** Format of the schema [name] if it is known, without asking the registry. */
    fun known(name: String): KafkaRegistryFormat? = formats[name]

    /**
     * Format of the schema [name], or `null` when it can't be told because the schema is gone or its load failed.
     * Asks the registry right away, also while a bulk request is still busy with other schemas.
     */
    suspend fun formatOf(name: String): KafkaRegistryFormat? =
        known(name) ?: load(name)?.let(::formatIn)

    /**
     * Reports the formats of the [names]: at once those that are known, and the others as soon as they are found,
     * the ones of [first] before the rest. Returns when all of the [names] are done.
     *
     * Failed loads are skipped, they must not take the others down with them. A schema that could not be resolved
     * a short while ago is not asked for again, unless it is one of the [first].
     */
    suspend fun resolveAll(
        names: Collection<String>,
        first: Collection<String> = emptyList(),
        onResolved: (name: String, format: KafkaRegistryFormat) -> Unit = { _, _ -> }
    ) {
        val urgent = first.filter { it in names }.toSet()
        val pending = mutableListOf<String>()
        (urgent + names).forEach { name ->
            // Decided in one go, so a format that becomes known meanwhile is not missed.
            val format = known(name)
            if (format != null) onResolved(name, format)
            else if (name in urgent || !failedRecently(name)) pending += name
        }
        if (pending.isEmpty()) return

        logger.timed("Resolving the formats of ${pending.size} schemas") {
            coroutineScope {
                pending.forEach { name ->
                    launch {
                        // inFlight is read before known: a load records its format before it leaves inFlight,
                        // so a load that ends in between is seen in one of the two and no slot is waited for in vain.
                        val running = inFlight[name]
                        val format = when {
                            name in urgent -> formatOf(name)
                            // Somebody is loading it already, no need to wait for a slot to join them.
                            running != null -> running.await()?.let(::formatIn)
                            else -> known(name) ?: bulkSlots.withPermit { formatOf(name) }
                        }
                        format?.let { onResolved(name, it) }
                    }
                }
            }
        }
    }

    /**
     * Loads the details of the [names] from the registry, as far as they can be loaded.
     * Unlike [formatOf] this does not rely on what is known: it is meant for refreshing.
     */
    suspend fun refreshAll(names: Collection<String>): Map<String, KafkaSchemaInfo> = coroutineScope {
        names.distinct().map { name ->
            async { bulkSlots.withPermit { load(name) }?.let { name to it } }
        }.awaitAll().filterNotNull().toMap()
    }

    fun put(name: String, format: KafkaRegistryFormat) {
        unresolved.remove(name)
        if (format == KafkaRegistryFormat.UNKNOWN) formats.remove(name) else formats[name] = format
    }

    fun remove(name: String) {
        unresolved.remove(name)
        formats.remove(name)
    }

    fun clear() {
        unresolved.clear()
        formats.clear()
    }

    /** Stops the requests that are running. */
    fun close() {
        scope.cancel()
        unresolved.clear()
        formats.clear()
    }

    private fun failedRecently(name: String): Boolean {
        val failedAt = unresolved[name] ?: return false
        return clock() - failedAt < retryUnresolvedAfterMillis
    }

    /** One request per schema at a time: whoever asks while it runs gets its outcome. `null` if it failed. */
    private suspend fun load(name: String): KafkaSchemaInfo? = flight(name).await()

    private fun flight(name: String): Deferred<KafkaSchemaInfo?> {
        inFlight[name]?.let { return it }

        val created = scope.async(start = CoroutineStart.LAZY) { fetch(name) }
        inFlight.putIfAbsent(name, created)?.let {
            created.cancel()
            return it
        }
        created.invokeOnCompletion { inFlight.remove(name, created) }
        created.start()
        return created
    }

    // A single schema that can't be loaded must not take the whole list down with it.
    private suspend fun fetch(name: String): KafkaSchemaInfo? = try {
        logger.timed("Loading schema '$name'") { loadSchema(name) }.also { remember(name, it) }
    } catch (e: ProcessCanceledException) {
        throw e
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        logger.warn("Failed to load schema '$name'", e)
        unresolved[name] = clock()
        null
    }

    // Entries without loaded details say nothing about the format, so what is known stays as it is.
    private fun remember(name: String, schema: KafkaSchemaInfo) {
        val format = schema.takeIf { it.hasLoadedDetails() }?.type
        when {
            format == null -> unresolved[name] = clock()
            format == KafkaRegistryFormat.UNKNOWN -> {
                formats.remove(name)
                unresolved[name] = clock()
            }

            else -> {
                formats[name] = format
                unresolved.remove(name)
            }
        }
    }

    private fun formatIn(schema: KafkaSchemaInfo): KafkaRegistryFormat? =
        schema.takeIf { it.hasLoadedDetails() }?.type?.takeIf { it != KafkaRegistryFormat.UNKNOWN }

    companion object {
        // The JDK keeps up to 5 idle connections per host for reuse, more concurrent requests would need new ones.
        const val DEFAULT_PARALLELISM = 5

        // Long enough for a list that is painted again and again, short enough to see a schema that became readable.
        const val DEFAULT_RETRY_UNRESOLVED_AFTER_MS = 60_000L
    }
}
