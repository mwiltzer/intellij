package io.confluent.intellijplugin.data

import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class SchemaFormatResolverTest {

    /**
     * Registry stand-in: knows the [formats], records what was requested and how many requests ran at once.
     * A request for a name with a gate waits until the gate is opened.
     */
    private class FakeRegistry(
        private val formats: Map<String, KafkaRegistryFormat> = emptyMap(),
        @Volatile var latencyMs: Long = 0,
        private val failing: Set<String> = emptySet()
    ) {
        val requested = ConcurrentLinkedQueue<String>()
        val maxConcurrentRequests = AtomicInteger()
        private val running = AtomicInteger()
        private val gates = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

        fun hold(name: String) = CompletableDeferred<Unit>().also { gates[name] = it }

        suspend fun load(name: String): KafkaSchemaInfo {
            requested += name
            maxConcurrentRequests.accumulateAndGet(running.incrementAndGet()) { a, b -> maxOf(a, b) }
            try {
                gates[name]?.await()
                if (latencyMs > 0) delay(latencyMs)
                if (name in failing) error("registry unavailable")
                val format = formats[name] ?: return KafkaSchemaInfo.createEmpty(name)
                return KafkaSchemaInfo(name = name, type = format, version = 1)
            } finally {
                running.decrementAndGet()
            }
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @AfterEach
    fun stopScope() {
        scope.cancel()
    }

    private fun resolverFor(registry: FakeRegistry, parallelism: Int = 4) =
        SchemaFormatResolver(parallelism, scope) { registry.load(it) }

    private fun names(count: Int) = (1..count).map { "subject-$it-value" }

    private fun avro(vararg names: String) = names.associateWith { KafkaRegistryFormat.AVRO }

    @Nested
    inner class `formatOf` {

        @Test
        fun `should load the format of a schema only once`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("orders-value"))
            val resolver = resolverFor(registry)

            assertEquals(KafkaRegistryFormat.AVRO, resolver.formatOf("orders-value"))
            assertEquals(KafkaRegistryFormat.AVRO, resolver.formatOf("orders-value"))

            assertEquals(listOf("orders-value"), registry.requested.toList())
        }

        @Test
        fun `should not remember a format that could not be loaded`() = runBlocking<Unit> {
            val registry = FakeRegistry(failing = setOf("bad-value"), formats = avro("bad-value"))
            val resolver = resolverFor(registry)

            assertNull(resolver.formatOf("bad-value"))
            assertNull(resolver.formatOf("bad-value"))

            assertEquals(2, registry.requested.size)
        }

        @Test
        fun `should not take a schema without loaded details for a format`() = runBlocking<Unit> {
            val registry = FakeRegistry() // answers every name with a soft deleted placeholder
            val resolver = resolverFor(registry)

            assertNull(resolver.formatOf("missing-value"))
            assertNull(resolver.known("missing-value"))
        }

        @Test
        fun `should not take unknown for a format`() = runBlocking<Unit> {
            val resolver = SchemaFormatResolver(scope = scope) {
                KafkaSchemaInfo(it, type = KafkaRegistryFormat.UNKNOWN, version = 1)
            }

            assertNull(resolver.formatOf("a-value"))
            assertNull(resolver.known("a-value"))
        }

        @Test
        fun `should share one request between callers asking at the same time`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val gate = registry.hold("a-value")
            val resolver = resolverFor(registry)

            val callers = (1..3).map { async(Dispatchers.Default) { resolver.formatOf("a-value") } }
            gate.complete(Unit)

            assertEquals(List(3) { KafkaRegistryFormat.AVRO }, withTimeout(5_000) { callers.awaitAll() })
            assertEquals(listOf("a-value"), registry.requested.toList())
        }

        @Test
        fun `should keep loading for the others when a caller gives up`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val gate = registry.hold("a-value")
            val resolver = resolverFor(registry)

            val quitter = async(Dispatchers.Default) { withTimeoutOrNull(100) { resolver.formatOf("a-value") } }
            val stayer = async(Dispatchers.Default) { resolver.formatOf("a-value") }
            assertNull(quitter.await())
            gate.complete(Unit)

            assertEquals(KafkaRegistryFormat.AVRO, withTimeout(5_000) { stayer.await() })
            assertEquals(listOf("a-value"), registry.requested.toList())
        }

        @Test
        fun `should not wait for a bulk request that is busy with other schemas`() = runBlocking<Unit> {
            val bulkNames = names(4)
            val registry = FakeRegistry(avro("wanted-value", *bulkNames.toTypedArray()))
            val gates = bulkNames.map { registry.hold(it) }
            val resolver = resolverFor(registry, parallelism = 1)
            val bulk = async(Dispatchers.Default) { resolver.resolveAll(bulkNames) }
            withTimeout(5_000) { while (registry.requested.isEmpty()) delay(5) }

            assertEquals(KafkaRegistryFormat.AVRO, withTimeout(5_000) { resolver.formatOf("wanted-value") })

            gates.forEach { it.complete(Unit) }
            withTimeout(5_000) { bulk.await() }
        }
    }

    @Nested
    inner class `resolveAll` {

        @Test
        fun `should report the formats it finds`() = runBlocking<Unit> {
            val registry = FakeRegistry(
                mapOf(
                    "a-value" to KafkaRegistryFormat.AVRO,
                    "b-value" to KafkaRegistryFormat.JSON,
                    "c-value" to KafkaRegistryFormat.PROTOBUF
                )
            )
            val resolver = resolverFor(registry)
            val found = ConcurrentHashMap<String, KafkaRegistryFormat>()

            resolver.resolveAll(listOf("a-value", "b-value", "c-value")) { name, format -> found[name] = format }

            assertEquals(
                mapOf(
                    "a-value" to KafkaRegistryFormat.AVRO,
                    "b-value" to KafkaRegistryFormat.JSON,
                    "c-value" to KafkaRegistryFormat.PROTOBUF
                ),
                found
            )
            assertEquals(KafkaRegistryFormat.JSON, resolver.known("b-value"))
        }

        @Test
        fun `should report the formats that are known without requesting them`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value", "b-value"))
            val resolver = resolverFor(registry)
            resolver.put("a-value", KafkaRegistryFormat.AVRO)
            val found = ConcurrentLinkedQueue<String>()

            resolver.resolveAll(listOf("a-value", "b-value")) { name, _ -> found += name }

            assertEquals(listOf("b-value"), registry.requested.toList())
            assertEquals(listOf("a-value", "b-value"), found.toList())
        }

        @Test
        fun `should leave a schema that fails to load unresolved without failing the others`() = runBlocking<Unit> {
            val registry = FakeRegistry(
                formats = avro("good-value", "bad-value"),
                failing = setOf("bad-value")
            )
            val resolver = resolverFor(registry)
            val found = ConcurrentHashMap<String, KafkaRegistryFormat>()

            resolver.resolveAll(listOf("good-value", "bad-value", "missing-value")) { name, format ->
                found[name] = format
            }

            assertEquals(mapOf("good-value" to KafkaRegistryFormat.AVRO), found)
        }

        @Test
        fun `should load several schemas at the same time but no more than the given number`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro(*names(20).toTypedArray()), latencyMs = 20)
            val resolver = resolverFor(registry, parallelism = 4)

            resolver.resolveAll(names(20))

            assertEquals(4, registry.maxConcurrentRequests.get())
        }

        @Test
        fun `should resolve the urgent schemas although all slots are taken`() = runBlocking<Unit> {
            val all = names(5)
            val urgent = all.last()
            val registry = FakeRegistry(avro(*all.toTypedArray()))
            val gates = all.filter { it != urgent }.map { registry.hold(it) }
            val resolver = resolverFor(registry, parallelism = 1)
            val found = ConcurrentLinkedQueue<String>()

            val bulk = async(Dispatchers.Default) {
                resolver.resolveAll(all, first = listOf(urgent)) { name, _ -> found += name }
            }

            withTimeout(5_000) { while (urgent !in found) delay(5) }
            assertEquals(listOf(urgent), found.toList())

            gates.forEach { it.complete(Unit) }
            withTimeout(5_000) { bulk.await() }
            assertEquals(all.sorted(), found.toList().sorted())
        }

        @Test
        fun `should share the work between bulk requests that run at the same time`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro(*names(10).toTypedArray()), latencyMs = 20)
            val resolver = resolverFor(registry)

            listOf(
                async(Dispatchers.Default) { resolver.resolveAll(names(10)) },
                async(Dispatchers.Default) { resolver.resolveAll(names(10)) }
            ).awaitAll()

            assertEquals(names(10).sorted(), registry.requested.toList().sorted())
        }

        @Test
        fun `should join a schema that is being loaded without waiting for a free slot`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("slow-value", "joined-value"))
            val slowGate = registry.hold("slow-value")
            val joinedGate = registry.hold("joined-value")
            val resolver = resolverFor(registry, parallelism = 1)
            val slow = async(Dispatchers.Default) { resolver.refreshAll(listOf("slow-value")) }
            val loading = async(Dispatchers.Default) { resolver.formatOf("joined-value") }
            withTimeout(5_000) { while (registry.requested.size < 2) delay(5) }

            val joining = async(Dispatchers.Default) { resolver.resolveAll(listOf("joined-value")) }
            // Gives the resolver time to join the running load. If it is late, it reads the format of the
            // finished load instead; both ways it must not queue for the slot that the slow schema holds.
            delay(100)
            joinedGate.complete(Unit)

            withTimeout(5_000) { joining.await() }
            assertEquals(KafkaRegistryFormat.AVRO, loading.await())
            assertFalse(slow.isCompleted)
            slowGate.complete(Unit)
            slow.await()
        }

        @Test
        fun `should stop on cancellation and stay usable afterwards`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val gate = registry.hold("a-value")
            val resolver = resolverFor(registry)

            assertThrows<CancellationException> {
                withTimeout(100) { resolver.resolveAll(listOf("a-value")) }
            }

            val found = ConcurrentHashMap<String, KafkaRegistryFormat>()
            val again = async(Dispatchers.Default) {
                resolver.resolveAll(listOf("a-value")) { name, format -> found[name] = format }
            }
            gate.complete(Unit)

            withTimeout(5_000) { again.await() }
            assertEquals(mapOf("a-value" to KafkaRegistryFormat.AVRO), found)
            // The request of the call that was cancelled served the next one.
            assertEquals(listOf("a-value"), registry.requested.toList())
        }
    }

    @Nested
    inner class `refreshAll` {

        @Test
        fun `should load the details of every schema no matter what is known`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value", "b-value"))
            val resolver = resolverFor(registry)
            resolver.put("a-value", KafkaRegistryFormat.AVRO)

            val loaded = resolver.refreshAll(listOf("a-value", "b-value"))

            assertEquals(setOf("a-value", "b-value"), loaded.keys)
            assertEquals(1L, loaded.getValue("a-value").version)
            assertEquals(listOf("a-value", "b-value"), registry.requested.toList().sorted())
        }

        @Test
        fun `should leave out the schemas that fail to load`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("good-value", "bad-value"), failing = setOf("bad-value"))
            val resolver = resolverFor(registry)

            val loaded = resolver.refreshAll(listOf("good-value", "bad-value"))

            assertEquals(setOf("good-value"), loaded.keys)
        }

        @Test
        fun `should load no more schemas at the same time than the given number`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro(*names(20).toTypedArray()), latencyMs = 20)
            val resolver = resolverFor(registry, parallelism = 3)

            resolver.refreshAll(names(20))

            assertEquals(3, registry.maxConcurrentRequests.get())
        }

        @Test
        fun `should replace the formats it knew with the refreshed ones`() = runBlocking<Unit> {
            val resolver = SchemaFormatResolver(scope = scope) { name ->
                when (name) {
                    "changed-value" -> KafkaSchemaInfo(name, type = KafkaRegistryFormat.PROTOBUF, version = 2)
                    "unknown-value" -> KafkaSchemaInfo(name, type = KafkaRegistryFormat.UNKNOWN, version = 1)
                    else -> KafkaSchemaInfo.createEmpty(name)
                }
            }
            resolver.put("changed-value", KafkaRegistryFormat.AVRO)
            resolver.put("unknown-value", KafkaRegistryFormat.AVRO)
            resolver.put("failed-value", KafkaRegistryFormat.JSON)

            resolver.refreshAll(listOf("changed-value", "unknown-value", "failed-value"))

            assertEquals(KafkaRegistryFormat.PROTOBUF, resolver.known("changed-value"))
            assertNull(resolver.known("unknown-value"))
            // What a failed refresh did not tell is not forgotten.
            assertEquals(KafkaRegistryFormat.JSON, resolver.known("failed-value"))
        }

        @Test
        fun `should share its requests with callers that want one format`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val gate = registry.hold("a-value")
            val resolver = resolverFor(registry)

            val refreshing = async(Dispatchers.Default) { resolver.refreshAll(listOf("a-value")) }
            withTimeout(5_000) { while (registry.requested.isEmpty()) delay(5) }
            val asking = async(Dispatchers.Default) { resolver.formatOf("a-value") }
            delay(50)
            gate.complete(Unit)

            assertEquals(KafkaRegistryFormat.AVRO, withTimeout(5_000) { asking.await() })
            assertEquals(setOf("a-value"), withTimeout(5_000) { refreshing.await() }.keys)
            assertEquals(listOf("a-value"), registry.requested.toList())
        }
    }

    @Nested
    inner class `schemas that could not be resolved` {

        private val now = AtomicLong(1_000)

        private fun resolver(registry: FakeRegistry) =
            SchemaFormatResolver(4, scope, retryUnresolvedAfterMillis = 60_000, clock = now::get) { registry.load(it) }

        // The registry answers a name it does not know with a placeholder without details, like a subject it can't read.
        @Test
        fun `should not ask again for a schema without details when many schemas are resolved`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("good-value"))
            val resolver = resolver(registry)

            resolver.resolveAll(listOf("good-value", "unreadable-value"))
            resolver.resolveAll(listOf("good-value", "unreadable-value"))

            assertEquals(listOf("good-value", "unreadable-value"), registry.requested.toList().sorted())
        }

        @Test
        fun `should not ask again for a schema that failed to load when many schemas are resolved`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("bad-value"), failing = setOf("bad-value"))
            val resolver = resolver(registry)

            resolver.resolveAll(listOf("bad-value"))
            resolver.resolveAll(listOf("bad-value"))
            resolver.resolveAll(listOf("bad-value"))

            assertEquals(listOf("bad-value"), registry.requested.toList())
        }

        @Test
        fun `should ask again once the time to retry has passed`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("unreadable-value"))

            now.addAndGet(59_999)
            resolver.resolveAll(listOf("unreadable-value"))
            assertEquals(1, registry.requested.size)

            now.addAndGet(1)
            resolver.resolveAll(listOf("unreadable-value"))
            assertEquals(2, registry.requested.size)
        }

        @Test
        fun `should ask when a schema is asked for by name, whatever happened before`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("unreadable-value"))

            assertNull(resolver.formatOf("unreadable-value"))

            assertEquals(2, registry.requested.size)
        }

        @Test
        fun `should ask for the urgent schemas, whatever happened before`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("unreadable-value"))

            resolver.resolveAll(listOf("unreadable-value"), first = listOf("unreadable-value"))

            assertEquals(2, registry.requested.size)
        }

        @Test
        fun `should not ask again for the others when the unresolved ones are skipped`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value", "b-value"))
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("unreadable-value"))
            val found = ConcurrentLinkedQueue<String>()

            resolver.resolveAll(listOf("unreadable-value", "a-value", "b-value")) { name, _ -> found += name }

            assertEquals(listOf("a-value", "b-value"), found.toList().sorted())
            assertEquals(listOf("a-value", "b-value", "unreadable-value"), registry.requested.toList().sorted())
        }

        @Test
        fun `should report a schema once it was resolved by name although it failed before`() = runBlocking<Unit> {
            val calls = AtomicInteger()
            val resolver = SchemaFormatResolver(4, scope, 60_000, now::get) { name ->
                if (calls.incrementAndGet() == 1) error("registry unavailable")
                KafkaSchemaInfo(name, KafkaRegistryFormat.AVRO, 1)
            }
            resolver.resolveAll(listOf("a-value"))
            assertNull(resolver.known("a-value"))

            assertEquals(KafkaRegistryFormat.AVRO, resolver.formatOf("a-value"))

            val found = ConcurrentLinkedQueue<String>()
            resolver.resolveAll(listOf("a-value")) { name, _ -> found += name }
            assertEquals(listOf("a-value"), found.toList())
            assertEquals(2, calls.get())
        }

        @Test
        fun `should ask again after the schema was changed, removed or everything was cleared`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("a-value", "b-value", "c-value"))
            assertEquals(3, registry.requested.size)

            resolver.put("a-value", KafkaRegistryFormat.AVRO)
            resolver.remove("b-value")
            resolver.resolveAll(listOf("a-value", "b-value", "c-value"))
            // a-value is known, b-value was removed, c-value is still waiting for its time.
            assertEquals(listOf("a-value", "b-value", "b-value", "c-value"), registry.requested.toList().sorted())

            resolver.clear()
            resolver.resolveAll(listOf("c-value"))
            assertEquals(5, registry.requested.size)
        }

        @Test
        fun `should not skip schemas of a refresh`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolver(registry)
            resolver.resolveAll(listOf("unreadable-value"))

            resolver.refreshAll(listOf("unreadable-value"))

            assertEquals(2, registry.requested.size)
        }
    }

    @Nested
    inner class `known formats` {

        @Test
        fun `known should tell nothing about schemas it has not seen`() {
            val resolver = resolverFor(FakeRegistry())

            assertNull(resolver.known("a-value"))
        }

        @Test
        fun `put should make a format known without asking the registry`() = runBlocking<Unit> {
            val registry = FakeRegistry()
            val resolver = resolverFor(registry)

            resolver.put("a-value", KafkaRegistryFormat.JSON)

            assertEquals(KafkaRegistryFormat.JSON, resolver.known("a-value"))
            assertEquals(KafkaRegistryFormat.JSON, resolver.formatOf("a-value"))
            assertTrue(registry.requested.isEmpty())
        }

        @Test
        fun `put should not take unknown for a format`() {
            val resolver = resolverFor(FakeRegistry())
            resolver.put("a-value", KafkaRegistryFormat.AVRO)

            resolver.put("a-value", KafkaRegistryFormat.UNKNOWN)

            assertNull(resolver.known("a-value"))
        }

        @Test
        fun `remove and clear should make the resolver request the format again`() = runBlocking<Unit> {
            val registry = FakeRegistry(
                mapOf("a-value" to KafkaRegistryFormat.AVRO, "b-value" to KafkaRegistryFormat.JSON)
            )
            val resolver = resolverFor(registry)
            resolver.put("a-value", KafkaRegistryFormat.AVRO)
            resolver.put("b-value", KafkaRegistryFormat.JSON)

            resolver.remove("a-value")
            resolver.formatOf("a-value")
            resolver.formatOf("b-value")
            assertEquals(listOf("a-value"), registry.requested.toList())

            resolver.clear()
            resolver.formatOf("b-value")
            assertEquals(listOf("a-value", "b-value"), registry.requested.toList())
        }

        @Test
        fun `close should stop the requests that are running`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            registry.hold("a-value")
            val resolver = resolverFor(registry)
            val waiting = async(Dispatchers.Default) { runCatching { resolver.formatOf("a-value") } }
            withTimeout(5_000) { while (registry.requested.isEmpty()) delay(5) }

            resolver.close()

            assertTrue(withTimeout(5_000) { waiting.await() }.exceptionOrNull() is CancellationException)
        }
    }

    @Nested
    inner class `known details` {

        private val now = AtomicLong(1_000)

        private fun resolver(registry: FakeRegistry, infoTtlMillis: Long = 300_000) =
            SchemaFormatResolver(4, scope, 60_000, now::get, infoTtlMillis) { registry.load(it) }

        @Test
        fun `knownInfo should tell nothing about schemas it has not loaded`() {
            assertNull(resolver(FakeRegistry()).knownInfo("a-value"))
        }

        @Test
        fun `should know the details a load brought`() = runBlocking<Unit> {
            val resolver = resolver(FakeRegistry(avro("a-value")))

            resolver.formatOf("a-value")

            assertEquals(KafkaSchemaInfo("a-value", KafkaRegistryFormat.AVRO, 1), resolver.knownInfo("a-value"))
        }

        @Test
        fun `should know the details a refresh brought`() = runBlocking<Unit> {
            val resolver = resolver(FakeRegistry(avro("a-value", "b-value")))

            resolver.refreshAll(listOf("a-value", "b-value"))

            assertEquals(KafkaRegistryFormat.AVRO, resolver.knownInfo("a-value")?.type)
            assertEquals(KafkaRegistryFormat.AVRO, resolver.knownInfo("b-value")?.type)
        }

        @Test
        fun `should not know details of a schema that has none`() = runBlocking<Unit> {
            val resolver = resolver(FakeRegistry())

            resolver.formatOf("unreadable-value")

            assertNull(resolver.knownInfo("unreadable-value"))
        }

        @Test
        fun `should forget the details after the time to keep them`() = runBlocking<Unit> {
            val resolver = resolver(FakeRegistry(avro("a-value")), infoTtlMillis = 1_000)
            resolver.formatOf("a-value")

            now.addAndGet(999)
            assertEquals(KafkaRegistryFormat.AVRO, resolver.knownInfo("a-value")?.type)

            now.addAndGet(1)
            assertNull(resolver.knownInfo("a-value"))
            // The format does not change with the version, so it is still known.
            assertEquals(KafkaRegistryFormat.AVRO, resolver.known("a-value"))
        }

        @Test
        fun `put, remove and clear should forget the details`() = runBlocking<Unit> {
            val resolver = resolver(FakeRegistry(avro("a-value", "b-value", "c-value")))
            resolver.refreshAll(listOf("a-value", "b-value", "c-value"))

            resolver.put("a-value", KafkaRegistryFormat.AVRO)
            resolver.remove("b-value")
            assertNull(resolver.knownInfo("a-value"))
            assertNull(resolver.knownInfo("b-value"))
            assertEquals(KafkaRegistryFormat.AVRO, resolver.knownInfo("c-value")?.type)

            resolver.clear()
            assertNull(resolver.knownInfo("c-value"))
        }

        @Test
        fun `detailsOf should load the details that are not known and keep those that are`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value", "b-value", "c-value"))
            val resolver = resolver(registry)
            resolver.formatOf("a-value")
            registry.requested.clear()

            val details = resolver.detailsOf(listOf("a-value", "b-value", "c-value"))

            assertEquals(setOf("a-value", "b-value", "c-value"), details.keys)
            assertTrue(details.values.all { it.type == KafkaRegistryFormat.AVRO && it.version == 1L })
            assertEquals(listOf("b-value", "c-value"), registry.requested.toList().sorted())
        }

        @Test
        fun `detailsOf should not ask again for what it loaded`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value", "b-value"))
            val resolver = resolver(registry)

            resolver.detailsOf(listOf("a-value", "b-value"))
            resolver.detailsOf(listOf("b-value", "a-value"))

            assertEquals(listOf("a-value", "b-value"), registry.requested.toList().sorted())
        }

        @Test
        fun `detailsOf should ask again for details that are too old`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val resolver = resolver(registry, infoTtlMillis = 1_000)
            resolver.detailsOf(listOf("a-value"))

            now.addAndGet(1_000)
            resolver.detailsOf(listOf("a-value"))

            assertEquals(2, registry.requested.size)
        }

        @Test
        fun `detailsOf should count a name once`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))

            val details = resolver(registry).detailsOf(listOf("a-value", "a-value"))

            assertEquals(1, details.size)
            assertEquals(1, registry.requested.size)
        }

        @Test
        fun `detailsOf should answer a schema without details with an empty one and not ask again for a while`() =
            runBlocking<Unit> {
                val registry = FakeRegistry()
                val resolver = resolver(registry)

                val first = resolver.detailsOf(listOf("unreadable-value"))
                val second = resolver.detailsOf(listOf("unreadable-value"))

                assertEquals(KafkaSchemaInfo.createEmpty("unreadable-value"), first["unreadable-value"])
                assertEquals(KafkaSchemaInfo.createEmpty("unreadable-value"), second["unreadable-value"])
                assertEquals(1, registry.requested.size)

                now.addAndGet(60_000)
                resolver.detailsOf(listOf("unreadable-value"))
                assertEquals(2, registry.requested.size)
            }

        @Test
        fun `detailsOf should leave out a schema whose request failed, and answer it empty for the next minute`() =
            runBlocking<Unit> {
                val registry = FakeRegistry(avro("bad-value", "good-value"), failing = setOf("bad-value"))
                val resolver = resolver(registry)

                val first = resolver.detailsOf(listOf("bad-value", "good-value"))
                val second = resolver.detailsOf(listOf("bad-value", "good-value"))

                assertEquals(setOf("good-value"), first.keys)
                assertEquals(KafkaSchemaInfo.createEmpty("bad-value"), second["bad-value"])
                assertEquals(listOf("bad-value", "good-value"), registry.requested.toList().sorted())
            }

        @Test
        fun `detailsOf should load a few schemas at a time`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro(*names(20).toTypedArray()), latencyMs = 10)
            val resolver = SchemaFormatResolver(3, scope) { registry.load(it) }

            resolver.detailsOf(names(20))

            assertEquals(20, registry.requested.size)
            assertTrue(registry.maxConcurrentRequests.get() <= 3, "at most 3 at once, was ${registry.maxConcurrentRequests}")
        }

        @Test
        fun `detailsOf should share the request with somebody who asks for the same schema`() = runBlocking<Unit> {
            val registry = FakeRegistry(avro("a-value"))
            val gate = registry.hold("a-value")
            val resolver = resolver(registry)

            val byName = async(Dispatchers.Default) { resolver.formatOf("a-value") }
            withTimeout(5_000) { while (registry.requested.isEmpty()) delay(5) }
            val details = async(Dispatchers.Default) { resolver.detailsOf(listOf("a-value")) }
            gate.complete(Unit)

            assertEquals(KafkaRegistryFormat.AVRO, withTimeout(5_000) { byName.await() })
            assertEquals(setOf("a-value"), withTimeout(5_000) { details.await() }.keys)
            assertEquals(1, registry.requested.size)
        }
    }
}
