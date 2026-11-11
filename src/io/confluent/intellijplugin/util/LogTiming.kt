package io.confluent.intellijplugin.util

import com.intellij.openapi.application.EDT
import com.intellij.openapi.diagnostic.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.TimeSource

/** Work that takes at least this long is worth a line in idea.log. */
internal const val SLOW_MILLIS = 1_000L

/**
 * Runs [block] and logs how long it took, so that a sluggish plugin leaves a trace in idea.log without flooding it:
 * at INFO when it took at least [slowAfterMillis], otherwise at DEBUG.
 *
 * [block] may suspend, and what it returns or throws, including a cancellation, is passed on as it is.
 * When it did not return, the line says "stopped after" instead of "took".
 */
internal inline fun <T> Logger.timed(
    what: String,
    slowAfterMillis: Long = SLOW_MILLIS,
    timeSource: TimeSource = TimeSource.Monotonic,
    block: () -> T
): T {
    val started = timeSource.markNow()
    var returned = false
    try {
        return block().also { returned = true }
    } finally {
        val millis = started.elapsedNow().inWholeMilliseconds
        val message = if (returned) "$what took $millis ms" else "$what stopped after $millis ms"
        if (millis >= slowAfterMillis) info(message) else debug(message)
    }
}

/**
 * Like [timed] for work that runs on the UI thread: it tells how much of the time went by before the UI thread was free
 * to start it. A UI thread that is busy with something else holds the work up as much as the work itself does, and the
 * line has to tell which of the two it was.
 *
 * What [block] returns or throws, including a cancellation, is passed on as it is.
 */
internal suspend inline fun <T> Logger.timedOnEdt(
    what: String,
    slowAfterMillis: Long = SLOW_MILLIS,
    timeSource: TimeSource = TimeSource.Monotonic,
    crossinline block: () -> T
): T {
    val asked = timeSource.markNow()
    var waitedMillis = -1L
    var returned = false
    try {
        return withContext(Dispatchers.EDT) {
            waitedMillis = asked.elapsedNow().inWholeMilliseconds
            block()
        }.also { returned = true }
    } finally {
        val millis = asked.elapsedNow().inWholeMilliseconds
        val outcome = if (returned) "took $millis ms" else "stopped after $millis ms"
        val message = if (waitedMillis >= 0) {
            "$what $outcome, $waitedMillis ms of it waiting for the UI thread"
        } else {
            "$what $outcome, before the UI thread took it"
        }
        if (millis >= slowAfterMillis) info(message) else debug(message)
    }
}
