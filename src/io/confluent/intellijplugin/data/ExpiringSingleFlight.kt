package io.confluent.intellijplugin.data

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async

/**
 * Loads a value once for all callers that ask at the same time, and keeps it for [ttlMillis].
 *
 * The load runs in [scope] and not in the caller, so a caller that gives up (the editor was closed, a newer
 * request replaced it) does not cancel the load for the others. A failed load is shared by the callers waiting
 * for it, but not remembered: the next call tries again. After [ttlMillis] the value is loaded again by the next
 * [get], but until then [peek] still hands out the old one.
 *
 * [ttlMillis] is asked every time the age of the value is looked at, so a value is kept for as long as it says now,
 * not for as long as it said when the value was loaded.
 */
internal class ExpiringSingleFlight<T : Any>(
    private val scope: CoroutineScope,
    private val ttlMillis: () -> Long,
    private val clock: () -> Long = System::currentTimeMillis,
    private val load: suspend () -> T
) {
    constructor(
        scope: CoroutineScope,
        ttlMillis: Long,
        clock: () -> Long = System::currentTimeMillis,
        load: suspend () -> T
    ) : this(scope, { ttlMillis }, clock, load)

    private class Cached<T>(val value: T, val loadedAt: Long)

    private val lock = Any()

    // All three are guarded by lock.
    private var cached: Cached<T>? = null
    private var inFlight: Deferred<T>? = null
    private var generation = 0L

    private fun Cached<T>.isFresh() = clock() - loadedAt < ttlMillis()

    suspend fun get(): T {
        val flight = synchronized(lock) {
            cached?.takeIf { it.isFresh() }?.let { return it.value }
            inFlight ?: startLoad()
        }
        return flight.await()
    }

    /**
     * Loads the value again, whatever [ttlMillis] says, e.g. for a refresh that the user asked for. A load that is
     * running right now is joined and not started a second time. Until the new value is there, [peek] still hands out
     * the old one.
     */
    suspend fun refresh(): T {
        val flight = synchronized(lock) { inFlight ?: startLoad() }
        return flight.await()
    }

    /**
     * Forgets the value, so the next [get] loads again, e.g. after the data was changed.
     * A load that is running right now still serves the callers waiting for it, but its result is not kept:
     * it may have been read before the change.
     */
    fun invalidate() = synchronized(lock) {
        generation++
        cached = null
        inFlight = null
    }

    /**
     * The value that was loaded last, also when it has expired, or `null` if there is none or it was [invalidate]d.
     * Never loads: it lets a caller show something right away while [get] brings the value up to date.
     */
    fun peek(): T? = synchronized(lock) { cached?.value }

    /**
     * The value that was loaded last if it has not expired yet, else `null`. Never loads: it is what [get] would hand
     * out without loading, for a caller that has a cheaper way than [get] when the value has to be loaded.
     */
    fun peekFresh(): T? = synchronized(lock) { cached?.takeIf { it.isFresh() }?.value }

    private fun startLoad(): Deferred<T> {
        val startedIn = generation
        val flight = scope.async(start = CoroutineStart.LAZY) {
            load().also { value ->
                synchronized(lock) {
                    if (generation == startedIn) cached = Cached(value, clock())
                }
            }
        }
        flight.invokeOnCompletion {
            synchronized(lock) { if (inFlight === flight) inFlight = null }
        }
        inFlight = flight
        flight.start()
        return flight
    }
}
