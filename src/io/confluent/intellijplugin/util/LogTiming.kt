package io.confluent.intellijplugin.util

import com.intellij.openapi.diagnostic.Logger
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
