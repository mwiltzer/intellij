package io.confluent.intellijplugin.common.editor

import com.intellij.openapi.application.EDT
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.progress.ProcessCanceledException
import com.intellij.openapi.ui.ComboBox
import io.confluent.intellijplugin.common.models.RegistrySchemaInEditor
import io.confluent.intellijplugin.core.util.invokeLater
import io.confluent.intellijplugin.data.SchemaFormats
import io.confluent.intellijplugin.util.timed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.awt.event.ItemEvent
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Fills the schema combo box of the consumer and producer editors without making the user wait for the registry.
 *
 * Listing the schemas and finding out the format of a schema both cost registry requests, and a registry can hold
 * thousands of schemas. So
 *  - the names that were listed before are shown at once, if there are any, while the registry is asked for the
 *    current ones: the list is there right away and is corrected when the answer differs,
 *  - formats are looked up only for the schemas that matter: the selected one and the one of the topic first, then the
 *    ones the list shows on screen ([wantFormat]), but not all of them.
 */
internal class SchemaComboBoxLoader(
    private val comboBox: ComboBox<RegistrySchemaInEditor>,
    private val source: SchemaFormats,
    private val scope: CoroutineScope,
    private val isKey: Boolean,
    private val reloadDelayMs: Long = RELOAD_DELAY_MS,
    private val logger: Logger = Logger.getInstance(SchemaComboBoxLoader::class.java),
    private val topic: () -> String?
) {
    private val which = if (isKey) "key" else "value"

    private val lock = Any()
    private var listing: Job? = null
    private var resolving: Job? = null

    // The schema that the user, or an editor restoring its settings, chose since the topic last changed. It stays
    // selected when the list is refreshed. A selection that only came with the list, the first schema or the one of
    // the topic, is no choice, so a better suggestion may replace it.
    @Volatile
    private var picked: String? = null

    // Rebuilding the list selects things on the way, which is no choice and no reason to look for their formats.
    private var updating = false

    private val repaintRequested = AtomicBoolean()

    // The rows on screen that wait for their format, by name, the ones painted last at the end.
    private val wanted = LinkedHashMap<String, RegistrySchemaInEditor>()
    private val wantedSignal = Channel<Unit>(Channel.CONFLATED)

    /** Whether the names are still being listed, an empty combo box says so then. */
    @Volatile
    var isListing = true
        private set

    private val renderer = SchemaListCellRenderer(isListing = { isListing }, formatWanted = ::wantFormat)

    init {
        comboBox.renderer = renderer
        // Whatever selects a schema other than the rebuild of the list is a choice, of the user or of a restored editor.
        comboBox.addItemListener {
            if (it.stateChange == ItemEvent.SELECTED && !updating) {
                picked = comboBox.item?.schemaName
                resolveSelected()
            }
        }
        scope.launch { resolveWanted() }
    }

    /** Lists the schemas right away: the first time, or because another topic was chosen. */
    fun load(topicChanged: Boolean = false) {
        // What was chosen for the previous topic does not count for this one.
        if (topicChanged) picked = null
        list(delayMs = 0)
    }

    /** Lists the schemas again because the registry changed. Changes that come in a burst are listed once. */
    fun reload() = list(delayMs = reloadDelayMs)

    /** Finds out the format of the selected schema now, instead of waiting for it to be on screen. */
    fun resolveSelected() {
        if (updating) return
        val item = comboBox.item ?: return
        if (item.schemaFormat != null) return

        scope.launch {
            source.getSchemaFormat(item.schemaName)?.let {
                item.schemaFormat = it
                requestRepaint()
            }
        }
    }

    /**
     * Asks for the format of a schema that the list shows without one. The list calls this for the rows it paints,
     * so the formats of the rows on screen come in, and the ones of thousands of schemas that are never looked at
     * are not asked for. Requests that come in a burst, e.g. while the list opens, are made together.
     */
    fun wantFormat(item: RegistrySchemaInEditor) {
        if (item.schemaFormat != null) return
        synchronized(wanted) {
            // A row that is painted again goes to the end: it is on screen, and the oldest ones may be gone.
            wanted.remove(item.schemaName)
            wanted[item.schemaName] = item
            if (wanted.size > MAX_WANTED) wanted.remove(wanted.keys.first())
        }
        wantedSignal.trySend(Unit)
    }

    private fun list(delayMs: Long) = synchronized(lock) {
        listing?.cancel()
        listing = scope.launch {
            if (delayMs > 0) delay(delayMs)

            // What was listed before is good enough to start with, the registry is asked for the current names meanwhile.
            val cached = source.getCachedSchemaNames()
            if (cached != null) startResolving(showOnEdt(cached, authoritative = false))

            val names = logger.timed("Getting the schema names for the $which combo box") {
                source.getSchemaNamesForEditor()
            }
            when {
                names != null -> startResolving(showOnEdt(names, authoritative = true))
                // The registry could not be asked. What is shown is all there is, and when nothing is, an empty list
                // is what ends the loading.
                isListing -> showOnEdt(emptyList(), authoritative = true)
            }
        }
    }

    /** Shows the [names] on the EDT. */
    private suspend fun showOnEdt(names: List<String>, authoritative: Boolean): List<RegistrySchemaInEditor> =
        withContext(Dispatchers.EDT) { show(names, authoritative) }

    // The formats are not part of the listing job: the next listing must not stop what is already being loaded.
    private fun startResolving(items: List<RegistrySchemaInEditor>) = synchronized(lock) {
        resolving?.cancel()
        resolving = scope.launch { resolve(items, urgent = true) }
    }

    private suspend fun resolveWanted() {
        for (signal in wantedSignal) {
            // Rows that only pass by while the list is scrolled are not worth a request, the ones that stay are.
            delay(WANTED_SETTLE_MS)
            val batch = synchronized(wanted) { wanted.values.toList().also { wanted.clear() } }
            resolve(batch, urgent = false)
        }
    }

    private suspend fun resolve(items: Collection<RegistrySchemaInEditor>, urgent: Boolean) {
        if (items.isEmpty()) return
        val byName = items.associateBy { it.schemaName }
        try {
            source.resolveSchemaFormats(byName.keys, first = if (urgent) byName.keys else emptyList()) { name, format ->
                byName[name]?.let {
                    it.schemaFormat = format
                    requestRepaint()
                }
            }
        } catch (e: ProcessCanceledException) {
            throw e
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.warn("Failed to look up schema formats for the $which combo box", e)
        }
    }

    /**
     * Shows the [names], with the schema that was chosen or that suits the topic selected.
     * Names that are not [authoritative], older than what the registry has, never take away a schema that was chosen.
     * The schemas that were selected, or that suit the topic, are returned: their formats are wanted first.
     */
    private fun show(names: List<String>, authoritative: Boolean): List<RegistrySchemaInEditor> {
        val current = comboBox.item?.schemaName?.takeIf { it.isNotEmpty() }
        val chosen = picked
        val listed = if (!authoritative && chosen != null && chosen !in names) names + chosen else names
        val preferred = chosen ?: KafkaEditorUtils.findTopicSchemaName(listed, topic().orEmpty(), isKey)

        val model = comboBox.model
        val shownBefore = (0 until model.size).map { model.getElementAt(it) }
        val itemsByName = shownBefore.associateBy { it.schemaName }
        val items = listed.distinct().sortedWith(ALPHABETICAL).map { name ->
            (itemsByName[name] ?: RegistrySchemaInEditor(name)).also { item ->
                source.getKnownSchemaFormat(name)?.let { item.schemaFormat = it }
            }
        }

        if (items != shownBefore) {
            quietly {
                comboBox.removeAllItems()
                items.forEach { comboBox.addItem(it) }
            }
        }

        // A rebuilt list selects its first item, so bring back what was selected, or what suits the topic.
        val suiting = preferred?.let { name -> items.firstOrNull { it.schemaName == name } }
        val selection = suiting ?: current?.let { name -> items.firstOrNull { it.schemaName == name } }
        when {
            selection != null -> if (comboBox.item != selection) quietly { comboBox.item = selection }
            // The schema that was chosen is not in the registry. Another one must not take its place unnoticed.
            chosen != null -> quietly { comboBox.selectedItem = null }
        }

        isListing = false
        comboBox.repaint()
        return listOfNotNull(comboBox.item, suiting).distinct()
    }

    private inline fun quietly(block: () -> Unit) {
        updating = true
        try {
            block()
        } finally {
            updating = false
        }
    }

    // Formats come in one after the other, so repaint once per round instead of once for every format.
    private fun requestRepaint() {
        if (repaintRequested.compareAndSet(false, true)) {
            invokeLater {
                repaintRequested.set(false)
                comboBox.repaint()
                // The list of an open combo box is a component of its own.
                renderer.repaintList()
            }
        }
    }

    companion object {
        // The registry model changes twice per refresh, and per schema while Confluent Cloud enriches them.
        const val RELOAD_DELAY_MS = 150L

        // A list shows a dozen rows. The ones that were painted before are most likely out of view.
        const val MAX_WANTED = 40
        const val WANTED_SETTLE_MS = 150L

        // Regardless of the case, which is only a tiebreaker, so that names that differ only by it do not change places.
        private val ALPHABETICAL = compareBy<String>({ it.lowercase() }, { it })
    }
}
