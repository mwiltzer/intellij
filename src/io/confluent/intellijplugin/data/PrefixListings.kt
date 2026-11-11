package io.confluent.intellijplugin.data

import kotlinx.coroutines.CoroutineScope

/**
 * What the registry listed for the prefixes that were asked for, see [SchemaListing.prefix]. Asking for the subjects
 * with a prefix costs a fraction of listing all of them, but is still a request, so a listing is kept for [ttlMillis]
 * and serves every prefix that begins with its own: when `ord` was listed, nothing is asked for `orders`.
 *
 * Requests for one prefix at the same time share a single request, which runs in [scope] and not in the caller, so a
 * caller that gives up does not cancel it for the others. At most [capacity] prefixes are kept, the ones that were asked
 * for longest ago go first.
 *
 * @param load the listing of the schemas that start with the prefix; `null` if the registry can not list by prefix,
 * which is then not asked again
 */
internal class PrefixListings(
    private val scope: CoroutineScope,
    private val ttlMillis: Long = TTL_MS,
    private val capacity: Int = CAPACITY,
    private val clock: () -> Long = System::currentTimeMillis,
    private val load: suspend (prefix: String) -> SchemaListing?
) {
    // Stands for a registry that can not list by prefix. It comes out of the single flight like any failure.
    private class Unsupported : Exception("The registry can not list the schemas by prefix")

    private val lock = Any()

    // Guarded by lock. In the order the prefixes were asked for last, which is what drops the oldest.
    private val flights = object : LinkedHashMap<String, ExpiringSingleFlight<SchemaListing>>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, ExpiringSingleFlight<SchemaListing>>?) =
            size > capacity
    }

    @Volatile
    private var unsupported = false

    @Volatile
    private var answered = false

    /**
     * Whether the registry lists by prefix: it did, and it was not told to be unable to later. That is what a caller that
     * relies on prefix listings can count on, while a registry that was not asked yet is not known to be able to.
     * It does not change when what was listed is cleared.
     */
    val canListByPrefix: Boolean get() = answered && !unsupported

    /** The listings that were loaded lately, for whatever prefix. Never asks the registry. */
    fun cached(): List<SchemaListing> {
        val peeked = synchronized(lock) { flights.values.mapNotNull { it.peek() } }
        return peeked.filter { clock() - it.loadedAt < ttlMillis }
    }

    /**
     * The listing of the schemas that start with [prefix], or of more than that. One that was loaded lately and has
     * them all is taken, unless a [refresh] is asked for, which lists the registry again for exactly this prefix.
     * `null` if the registry can not list by prefix.
     */
    suspend fun get(prefix: String, refresh: Boolean = false): SchemaListing? {
        if (unsupported) return null
        if (!refresh) SchemaListings.bestFor(prefix, cached())?.let { return it }

        val flight = synchronized(lock) {
            flights.getOrPut(prefix) {
                ExpiringSingleFlight(scope, ttlMillis, clock) { load(prefix) ?: throw Unsupported() }
            }
        }
        return try {
            (if (refresh) flight.refresh() else flight.get()).also { answered = true }
        } catch (_: Unsupported) {
            unsupported = true
            null
        }
    }

    /** Forgets everything that was listed, e.g. after the schemas of the registry were changed. */
    fun clear() = synchronized(lock) { flights.clear() }

    companion object {
        // Listing the schemas for a prefix costs little, and the list of the tool window follows what is typed.
        const val TTL_MS = 60_000L

        const val CAPACITY = 20

        private const val INITIAL_CAPACITY = 16
        private const val LOAD_FACTOR = 0.75f
    }
}
