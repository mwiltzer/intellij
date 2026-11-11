package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.SchemaUsage
import io.confluent.intellijplugin.util.catchFailures
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async

/**
 * Asks the registry once for each schema ID that records come with, for everyone who asks while it is asked
 * and for a while after.
 *
 * The records of a topic mostly share a handful of schemas, and the details of a record are shown with every click.
 * An answer is kept for [ttlMillis], a failure to get one only for [failureTtlMillis]: a registry that does not
 * answer is not asked again with every record, but is not given up on for long either. The request runs in [scope]
 * and not in the caller, so a caller that gives up (the next record was selected) does not cancel it for the others.
 * At most [capacity] IDs are remembered, the ones that were asked for longest ago go first.
 */
internal class RecordSchemaLookup(
    private val scope: CoroutineScope,
    private val ttlMillis: Long = TTL_MS,
    private val failureTtlMillis: Long = FAILURE_TTL_MS,
    private val capacity: Int = CAPACITY,
    private val clock: () -> Long = System::currentTimeMillis,
    private val fetch: suspend (schemaId: Int) -> List<SchemaUsage>?
) {
    private inner class Entry(schemaId: Int) {
        // Until the registry answered the entry has no end.
        @Volatile
        var expiresAt = Long.MAX_VALUE

        @Volatile
        var answer: List<SchemaUsage>? = null

        val flight: Deferred<List<SchemaUsage>?> = scope.async(start = CoroutineStart.LAZY) {
            val usages = catchFailures<List<SchemaUsage>?>(onFailure = { null }) { fetch(schemaId) }
            answer = usages
            expiresAt = clock() + if (usages != null) ttlMillis else failureTtlMillis
            usages
        }

        // A request that was cancelled for any other reason than the end of the scope would be given again forever.
        fun isUsable() = !flight.isCancelled && clock() < expiresAt
    }

    private val lock = Any()

    // Guarded by lock. In the order the IDs were asked for last, which is what drops the oldest.
    private val entries = object : LinkedHashMap<Int, Entry>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Int, Entry>?) = size > capacity
    }

    /** The subjects and versions of the schema [schemaId]; `null` if the registry could not tell. */
    suspend fun usagesOf(schemaId: Int): List<SchemaUsage>? {
        val entry = synchronized(lock) {
            entries[schemaId]?.takeIf { it.isUsable() } ?: Entry(schemaId).also { entries[schemaId] = it }
        }
        entry.flight.start()
        return entry.flight.await()
    }

    /** What the registry told about [schemaId] lately; `null` if it was not asked, did not tell yet or could not tell. */
    fun known(schemaId: Int): List<SchemaUsage>? = synchronized(lock) {
        entries[schemaId]?.takeIf { it.isUsable() }?.answer
    }

    /** Forgets everything, e.g. after the schemas of the registry were changed. */
    fun clear() = synchronized(lock) { entries.clear() }

    companion object {
        // The subjects that use a schema grow when it is registered once more, and shrink when a subject is deleted.
        const val TTL_MS = 5 * 60_000L

        const val FAILURE_TTL_MS = 30_000L

        const val CAPACITY = 200

        private const val INITIAL_CAPACITY = 16
        private const val LOAD_FACTOR = 0.75f
    }
}
