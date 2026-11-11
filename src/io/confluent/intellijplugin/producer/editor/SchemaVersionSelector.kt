package io.confluent.intellijplugin.producer.editor

import com.intellij.openapi.diagnostic.thisLogger
import com.intellij.openapi.progress.ProcessCanceledException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/** What the choice of a version of a registry schema shows. */
internal data class SchemaVersionState(
    val schemaName: String?,
    /** The versions the registry has, the newest first. Empty as long as the registry was not asked. */
    val versions: List<Long>,
    /** The version that was chosen, `null` for the latest one. */
    val selected: Long?
) {
    /** The latest version, as far as the registry told. */
    val latest: Long? get() = versions.firstOrNull()

    /**
     * What to choose from: the latest version, then the versions of the registry, the newest first. A version that
     * was chosen stays in the list until the registry says it is gone, so a saved choice can be shown before that.
     */
    val choices: List<Long?> get() = listOf(null) + (versions + listOfNotNull(selected)).distinct().sortedDescending()
}

/**
 * The version of a registry schema that a producer writes with: the latest one, unless somebody chose another.
 *
 * The choice belongs to the schema that is shown. Another schema starts with the latest version again. The list of
 * schemas is rebuilt now and then, which selects other schemas on the way and then the one that was shown. So a
 * change of the schema only counts when it holds for [settleMillis]. The versions are not asked for until [reload]
 * is called, e.g. when somebody opens the list of them.
 *
 * @param loadVersions the versions of a schema, or `null` when the registry could not be asked
 * @param onChange is told about every change of the state, from whatever thread made it
 */
internal class SchemaVersionSelector(
    private val scope: CoroutineScope,
    private val loadVersions: suspend (schemaName: String) -> List<Long>?,
    private val settleMillis: Long = SETTLE_MS,
    private val onChange: (SchemaVersionState) -> Unit
) {
    private val lock = Any()

    // All of these are guarded by lock.
    private var state = SchemaVersionState(schemaName = null, versions = emptyList(), selected = null)
    private var shown: String? = null
    private var restored: Pair<String, Long?>? = null
    private var settling: Job? = null
    private var loading: Job? = null

    /** The state as it is now. */
    fun state(): SchemaVersionState = synchronized(lock) { state }

    /**
     * The version a record of the schema [schemaName] is written with, `null` for the latest one. [schemaName] is
     * the schema that is shown, which may be a change that did not settle yet, or a saved choice that waits for it.
     */
    fun selectedFor(schemaName: String?): Long? = synchronized(lock) {
        when {
            schemaName.isNullOrEmpty() -> null
            schemaName == state.schemaName -> state.selected
            else -> restored?.takeIf { it.first == schemaName }?.second
        }
    }

    /** The schema that is shown changed, or looks like it did. `null`, or empty, is no schema. */
    fun schemaSelected(schemaName: String?) {
        synchronized(lock) {
            shown = schemaName?.takeIf { it.isNotEmpty() }
            settling?.cancel()
            settling = scope.launch {
                delay(settleMillis)
                commit()
            }
        }
    }

    /** A version was chosen for the schema that is shown. `null` is the latest one. */
    fun choose(version: Long?) {
        val changed = synchronized(lock) {
            if (state.schemaName == null || state.selected == version) return
            state.copy(selected = version).also { state = it }
        }
        onChange(changed)
    }

    /**
     * Brings back a choice that was saved for [schemaName]. It is shown with the schema, also when that is not
     * the schema that is shown yet, but one that is about to be.
     */
    fun restore(schemaName: String, version: Long?) {
        if (schemaName.isEmpty()) return

        val changed = synchronized(lock) {
            if (state.schemaName == schemaName) {
                if (state.selected == version) return
                state.copy(selected = version).also { state = it }
            } else {
                restored = schemaName to version
                return
            }
        }
        onChange(changed)
    }

    /** Asks the registry for the versions of the schema, so the list has the latest ones. */
    fun reload() {
        synchronized(lock) {
            val name = state.schemaName ?: return
            loading?.cancel()
            loading = scope.launch { load(name) }
        }
    }

    private fun commit() {
        val committed = synchronized(lock) {
            if (shown == state.schemaName) return
            val chosen = restored?.takeIf { it.first == shown }?.second
            restored = null
            loading?.cancel()
            SchemaVersionState(schemaName = shown, versions = emptyList(), selected = chosen).also { state = it }
        }
        onChange(committed)
    }

    private suspend fun load(name: String) {
        val loaded = try {
            loadVersions(name)
        } catch (e: ProcessCanceledException) {
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            thisLogger().warn("Failed to load the versions of schema '$name'", e)
            null
        }

        val changed = synchronized(lock) {
            // Another schema is shown by now.
            if (state.schemaName != name) return
            // What a request that failed does not tell is not taken back.
            val versions = loaded?.sortedDescending() ?: state.versions
            val selected = if (loaded == null) state.selected else state.selected?.takeIf { it in loaded }
            SchemaVersionState(name, versions, selected).takeIf { it != state }?.also { state = it }
        }
        changed?.let(onChange)
    }

    companion object {
        // Rebuilding the list of schemas is done in one go, so this is long enough to look past it.
        const val SETTLE_MS = 100L
    }
}
