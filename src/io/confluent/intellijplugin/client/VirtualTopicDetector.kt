package io.confluent.intellijplugin.client

import com.intellij.openapi.diagnostic.thisLogger
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.errors.AuthorizationException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Finds the virtual topics in a topic listing.
 *
 * Describing the configs of every topic on every listing is a large request on big clusters, and the answer
 * does not change for an existing topic. So each topic is described once and the answer is remembered.
 * Topics that are missing from a listing are forgotten, which makes a topic that is deleted and created again
 * get checked again. [clear] forgets everything, e.g. when the connection is established anew.
 *
 * [describeConfigs] answers for each topic on its own, so a topic that fails does not take the others with it.
 *  - A topic that the user may not describe can't be told to be virtual, so it is listed like a regular one. Asking
 *    again would fail the same way with every listing, so this is remembered like an answer.
 *  - Any other failure is not remembered: the topic is asked about again with the next listing.
 */
internal class VirtualTopicDetector(
    private val describeConfigs: (topicNames: List<String>) -> Map<String, Result<Collection<ConfigEntry>>>
) {
    private val logger = thisLogger()

    private val virtual = ConcurrentHashMap.newKeySet<String>()
    private val regular = ConcurrentHashMap.newKeySet<String>()
    private val generation = AtomicInteger()

    /**
     * Returns those of [topicNames] that are virtual. Only topics not seen before are described. Concurrent
     * callers queue up, so topics that are being described are not described a second time.
     */
    @Synchronized
    fun virtualAmong(topicNames: List<String>): Set<String> {
        val generationAtStart = generation.get()
        val listed = topicNames.toSet()
        virtual.retainAll(listed)
        regular.retainAll(listed)

        val unknown = topicNames.filter { it !in virtual && it !in regular }
        val classified = if (unknown.isEmpty()) emptyMap() else classify(unknown)

        // A clear() that happened while describing means the answers belong to the old connection.
        if (generation.get() == generationAtStart) {
            classified.forEach { (topic, isVirtual) -> (if (isVirtual) virtual else regular).add(topic) }
        }
        return topicNames.filterTo(mutableSetOf()) { it in virtual || classified[it] == true }
    }

    fun clear() {
        generation.incrementAndGet()
        virtual.clear()
        regular.clear()
    }

    /** Which of the [topicNames] are virtual, for those that could be told. */
    private fun classify(topicNames: List<String>): Map<String, Boolean> {
        val described = try {
            describeConfigs(topicNames)
        } catch (t: Throwable) {
            if (t is InterruptedException) Thread.currentThread().interrupt()
            logger.warn("Could not check ${topicNames.size} topics for virtual ones", t)
            return emptyMap()
        }

        val classified = HashMap<String, Boolean>()
        val notAllowed = mutableListOf<String>()
        var failed = 0
        var firstFailure: Pair<String, Throwable>? = null
        for ((topic, outcome) in described) {
            val entries = outcome.getOrNull()
            val cause = outcome.exceptionOrNull()
            when {
                entries != null -> classified[topic] = isVirtual(entries)
                cause is AuthorizationException -> {
                    classified[topic] = false
                    notAllowed += topic
                }

                else -> {
                    failed++
                    if (firstFailure == null && cause != null) firstFailure = topic to cause
                }
            }
        }

        if (notAllowed.isNotEmpty()) {
            logger.info(
                "Not allowed to describe the configs of ${notAllowed.size} topics, they are listed as regular topics: " +
                        notAllowed.take(MAX_TOPICS_LOGGED).joinToString() + if (notAllowed.size > MAX_TOPICS_LOGGED) ", ..." else ""
            )
        }
        firstFailure?.let { (topic, cause) ->
            logger.warn("Could not check $failed of ${topicNames.size} topics for virtual ones, for example '$topic'", cause)
        }
        return classified
    }

    private fun isVirtual(entries: Collection<ConfigEntry>) = entries.any {
        (it.name() == "confluent.topic.type" && it.value() == "virtual") ||
                (it.name() == "replication.factor" && it.value() == "0")
    }

    private companion object {
        const val MAX_TOPICS_LOGGED = 10
    }
}
