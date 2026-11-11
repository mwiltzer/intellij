package io.confluent.intellijplugin.client

import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class VirtualTopicDetectorTest {

    private val regularConfig = listOf(
        ConfigEntry("cleanup.policy", "delete"),
        ConfigEntry("replication.factor", "3"),
        ConfigEntry("confluent.topic.type", "regular"),
    )
    private val virtualConfig = listOf(ConfigEntry("confluent.topic.type", "virtual"))

    private val configs = mutableMapOf<String, List<ConfigEntry>>()

    // Topics the user may not describe, and topics that fail for another reason, each with its own outcome.
    private val denied = mutableSetOf<String>()
    private val broken = mutableMapOf<String, Throwable>()
    private val unanswered = mutableSetOf<String>()
    private val describeCalls = CopyOnWriteArrayList<List<String>>()
    private var failure: Throwable? = null
    private var beforeAnswering: () -> Unit = {}

    private val detector = VirtualTopicDetector { topicNames ->
        describeCalls.add(topicNames)
        beforeAnswering()
        failure?.let { throw it }
        topicNames.filter { it !in unanswered }.associateWith { topic ->
            when {
                topic in denied -> Result.failure(TopicAuthorizationException(setOf(topic)))
                topic in broken -> Result.failure(broken.getValue(topic))
                else -> Result.success(configs[topic] ?: regularConfig)
            }
        }
    }

    private fun awaitState(thread: Thread, state: Thread.State) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (thread.state != state) {
            check(System.nanoTime() < deadline) { "${thread.name} did not reach $state, is ${thread.state}" }
            Thread.sleep(5)
        }
    }

    @Test
    fun `should describe every topic of the first listing`() {
        val virtual = detector.virtualAmong(listOf("a", "b", "c"))

        assertEquals(emptySet<String>(), virtual)
        assertEquals(listOf(listOf("a", "b", "c")), describeCalls.toList())
    }

    @Test
    fun `should recognize the confluent virtual topic type`() {
        configs["b"] = virtualConfig

        assertEquals(setOf("b"), detector.virtualAmong(listOf("a", "b", "c")))
    }

    @Test
    fun `should recognize a replication factor of zero`() {
        configs["a"] = listOf(ConfigEntry("replication.factor", "0"))

        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))
    }

    @Test
    fun `should not treat other values of these configs as virtual`() {
        configs["a"] = listOf(ConfigEntry("confluent.topic.type", "regular"), ConfigEntry("replication.factor", "3"))
        configs["b"] = emptyList()

        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a", "b")))
    }

    @Test
    fun `should describe only the topics it has not seen before`() {
        detector.virtualAmong(listOf("a", "b"))
        detector.virtualAmong(listOf("a", "b", "c"))

        assertEquals(listOf(listOf("a", "b"), listOf("c")), describeCalls.toList())
    }

    @Test
    fun `should not describe again when the same topics are listed again`() {
        configs["a"] = virtualConfig

        val first = detector.virtualAmong(listOf("a", "b"))
        val second = detector.virtualAmong(listOf("a", "b"))

        assertEquals(setOf("a"), first)
        assertEquals(first, second)
        assertEquals(1, describeCalls.size)
    }

    @Test
    fun `should still report a known virtual topic when new topics are described`() {
        configs["a"] = virtualConfig
        detector.virtualAmong(listOf("a"))

        val virtual = detector.virtualAmong(listOf("a", "b"))

        assertEquals(setOf("a"), virtual)
        assertEquals(listOf(listOf("a"), listOf("b")), describeCalls.toList())
    }

    @Test
    fun `should only report virtual topics that are part of the listing`() {
        configs["a"] = virtualConfig
        configs["b"] = virtualConfig
        detector.virtualAmong(listOf("a", "b"))

        assertEquals(setOf("b"), detector.virtualAmong(listOf("b", "c")))
    }

    @Test
    fun `should neither describe nor fail for an empty listing`() {
        assertEquals(emptySet<String>(), detector.virtualAmong(emptyList()))
        assertTrue(describeCalls.isEmpty())
    }

    @Test
    fun `should not remember anything when describing fails`() {
        configs["a"] = virtualConfig
        failure = IllegalStateException("broker unavailable")

        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a", "b")))

        failure = null
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))
        assertEquals(listOf(listOf("a", "b"), listOf("a", "b")), describeCalls.toList())
    }

    @Test
    fun `should keep what it knows when a later describe fails`() {
        configs["a"] = virtualConfig
        detector.virtualAmong(listOf("a"))
        failure = IllegalStateException("broker unavailable")

        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))

        failure = null
        detector.virtualAmong(listOf("a", "b"))
        assertEquals(listOf(listOf("a"), listOf("b"), listOf("b")), describeCalls.toList())
    }

    @Test
    fun `should keep the interrupt flag when it is interrupted while describing`() {
        failure = InterruptedException("cancelled")

        try {
            assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a")))
            assertTrue(Thread.currentThread().isInterrupted)
        } finally {
            Thread.interrupted()
        }
    }

    @Test
    fun `should check a topic again after it disappeared from the listing`() {
        configs["a"] = virtualConfig
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a")))

        detector.virtualAmong(emptyList())

        configs["a"] = regularConfig
        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a")))
        assertEquals(listOf(listOf("a"), listOf("a")), describeCalls.toList())
    }

    @Test
    fun `should forget everything on clear`() {
        configs["a"] = virtualConfig
        detector.virtualAmong(listOf("a", "b"))

        detector.clear()

        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))
        assertEquals(listOf(listOf("a", "b"), listOf("a", "b")), describeCalls.toList())
    }

    @Test
    fun `should not remember answers that arrive after clear`() {
        configs["a"] = virtualConfig
        beforeAnswering = { detector.clear() }

        // The caller still gets the answer it asked for ...
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))

        // ... but it is not kept, because it belongs to the connection that was just replaced.
        beforeAnswering = {}
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))
        assertEquals(listOf(listOf("a", "b"), listOf("a", "b")), describeCalls.toList())
    }

    @Test
    fun `should still find the virtual topics when another topic may not be described`() {
        configs["b"] = virtualConfig
        denied += "a"

        assertEquals(setOf("b"), detector.virtualAmong(listOf("a", "b", "c")))
    }

    @Test
    fun `should list a topic that may not be described as a regular topic`() {
        denied += "a"

        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a", "b")))
    }

    @Test
    fun `should not ask again for a topic that may not be described`() {
        configs["b"] = virtualConfig
        denied += "a"
        detector.virtualAmong(listOf("a", "b"))

        val second = detector.virtualAmong(listOf("a", "b", "c"))

        // Asking again would fail the same way with every listing.
        assertEquals(setOf("b"), second)
        assertEquals(listOf(listOf("a", "b"), listOf("c")), describeCalls.toList())
    }

    @Test
    fun `should ask again for a topic that may not be described after the connection was replaced`() {
        denied += "a"
        detector.virtualAmong(listOf("a"))

        // The permissions may have changed with the new connection.
        detector.clear()
        denied.clear()
        configs["a"] = virtualConfig

        assertEquals(setOf("a"), detector.virtualAmong(listOf("a")))
        assertEquals(listOf(listOf("a"), listOf("a")), describeCalls.toList())
    }

    @Test
    fun `should treat a cluster that may not be described like a topic that may not be`() {
        broken["a"] = ClusterAuthorizationException("not allowed")
        configs["b"] = virtualConfig

        assertEquals(setOf("b"), detector.virtualAmong(listOf("a", "b")))

        detector.virtualAmong(listOf("a", "b"))
        assertEquals(1, describeCalls.size)
    }

    @Test
    fun `should keep the answers of the other topics when one fails for another reason`() {
        configs["b"] = virtualConfig
        broken["a"] = TimeoutException("no answer for a")

        assertEquals(setOf("b"), detector.virtualAmong(listOf("a", "b", "c")))

        broken.clear()
        detector.virtualAmong(listOf("a", "b", "c"))
        // Only the topic that failed is asked about again.
        assertEquals(listOf(listOf("a", "b", "c"), listOf("a")), describeCalls.toList())
    }

    @Test
    fun `should ask again for a topic that failed for another reason`() {
        configs["a"] = virtualConfig
        broken["a"] = TimeoutException("no answer")

        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a")))

        broken.clear()
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a")))
        assertEquals(listOf(listOf("a"), listOf("a")), describeCalls.toList())
    }

    @Test
    fun `should ask again for a topic that the answer left out`() {
        configs["a"] = virtualConfig
        unanswered += "a"

        assertEquals(emptySet<String>(), detector.virtualAmong(listOf("a", "b")))

        unanswered.clear()
        assertEquals(setOf("a"), detector.virtualAmong(listOf("a", "b")))
        assertEquals(listOf(listOf("a", "b"), listOf("a")), describeCalls.toList())
    }

    @Test
    fun `should describe a topic once for listings that overlap in time`() {
        configs["a"] = virtualConfig
        val describing = CountDownLatch(1)
        val release = CountDownLatch(1)
        beforeAnswering = {
            describing.countDown()
            release.await(10, TimeUnit.SECONDS)
        }
        val answers = CopyOnWriteArrayList<Set<String>>()

        val first = thread(name = "first-listing") { answers.add(detector.virtualAmong(listOf("a", "b"))) }
        assertTrue(describing.await(10, TimeUnit.SECONDS), "the first listing never started describing")
        val second = thread(name = "second-listing") { answers.add(detector.virtualAmong(listOf("a", "b"))) }
        awaitState(second, Thread.State.BLOCKED)

        release.countDown()
        first.join(10_000)
        second.join(10_000)

        assertEquals(1, describeCalls.size)
        assertEquals(listOf(setOf("a"), setOf("a")), answers.toList())
    }
}
