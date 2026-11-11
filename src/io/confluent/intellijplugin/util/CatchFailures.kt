package io.confluent.intellijplugin.util

import com.intellij.openapi.progress.ProcessCanceledException
import kotlinx.coroutines.CancellationException

/**
 * Runs [block] and turns a failure into the result of [onFailure].
 *
 * A cancellation is no failure: whoever cancelled the work waits for it to stop, so it is thrown on and [onFailure]
 * never sees it. That also keeps it out of the log, where the platform complains about it.
 */
internal inline fun <R> catchFailures(onFailure: (Throwable) -> R, block: () -> R): R = try {
    block()
} catch (e: ProcessCanceledException) {
    throw e
} catch (e: CancellationException) {
    throw e
} catch (t: Throwable) {
    onFailure(t)
}
