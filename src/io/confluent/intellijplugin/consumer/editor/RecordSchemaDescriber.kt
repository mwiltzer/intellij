package io.confluent.intellijplugin.consumer.editor

import io.confluent.intellijplugin.data.SchemaUsageSource
import io.confluent.intellijplugin.registry.RecordSchema
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/**
 * Shows what a record says about its schema: the ID at once, and the subjects and versions that use it as soon as the
 * registry told them, which is at once if it told before.
 *
 * Whatever the registry tells is shown only if the record it belongs to is still the one that is described: the
 * user may have selected another record while the registry was asked. [show] is called in [uiContext].
 */
internal class RecordSchemaDescriber(
    private val scope: CoroutineScope,
    private val source: SchemaUsageSource?,
    private val uiContext: CoroutineContext,
    private val show: (String) -> Unit
) {
    private val lock = Any()

    // Both guarded by lock. The number of the record that is described now, and the registry request for it.
    private var current = 0L
    private var lookup: Job? = null

    fun describe(schema: RecordSchema?, topic: String? = null) {
        val mine = synchronized(lock) {
            lookup?.cancel()
            lookup = null
            ++current
        }

        val id = schema?.id
        val known = id?.let { source?.getCachedSchemaUsages(it) }
        show(RecordSchemaText.of(schema, known, topic))

        if (id == null || source == null || known != null) return
        val request = scope.launch {
            val usages = source.getSchemaUsages(id)?.takeIf { it.isNotEmpty() } ?: return@launch
            withContext(uiContext) {
                if (isCurrent(mine)) show(RecordSchemaText.of(schema, usages, topic))
            }
        }
        synchronized(lock) { if (current == mine) lookup = request else request.cancel() }
    }

    private fun isCurrent(number: Long) = synchronized(lock) { current == number }
}
