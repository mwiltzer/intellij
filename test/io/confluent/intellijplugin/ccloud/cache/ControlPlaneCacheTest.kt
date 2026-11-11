package io.confluent.intellijplugin.ccloud.cache

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.ccloud.fetcher.ControlPlaneFetcher
import io.confluent.intellijplugin.ccloud.model.Cluster
import io.confluent.intellijplugin.ccloud.model.Environment
import io.confluent.intellijplugin.ccloud.model.SchemaRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

@TestApplication
class ControlPlaneCacheTest {

    private class FakeControlPlane : ControlPlaneFetcher {
        val clusterCalls = AtomicInteger()
        val registryCalls = AtomicInteger()
        var clusters: (String) -> List<Cluster> = { emptyList() }
        var registries: (String) -> SchemaRegistry? = { null }

        override suspend fun getEnvironments(): List<Environment> = emptyList()

        override suspend fun getKafkaClusters(envId: String): List<Cluster> {
            clusterCalls.incrementAndGet()
            return clusters(envId)
        }

        override suspend fun getSchemaRegistry(envId: String): SchemaRegistry? {
            registryCalls.incrementAndGet()
            return registries(envId)
        }
    }

    private fun cluster(id: String) = Cluster(
        id = id, displayName = id, cloudProvider = "AWS", region = "us-east-1",
        httpEndpoint = "https://$id.confluent.cloud"
    )

    private fun registry(id: String) = SchemaRegistry(
        id = id, displayName = id, cloudProvider = "AWS", region = "us-east-1",
        httpEndpoint = "https://$id.confluent.cloud"
    )

    private val control = FakeControlPlane()
    private val cache = ControlPlaneCache(null, control)

    @AfterEach
    fun disposeCache() {
        cache.dispose()
    }

    @Nested
    @DisplayName("clusters")
    inner class Clusters {

        @Test
        fun `should fetch the clusters of an environment once and then serve them from the cache`() {
            control.clusters = { listOf(cluster("lkc-1")) }

            val first = cache.getKafkaClusters("env-1")
            val second = cache.getKafkaClusters("env-1")

            assertEquals(listOf(cluster("lkc-1")), first)
            assertEquals(first, second)
            assertEquals(1, control.clusterCalls.get())
        }

        @Test
        fun `should keep the clusters of different environments apart`() {
            control.clusters = { env -> listOf(cluster("lkc-of-$env")) }

            assertEquals(listOf(cluster("lkc-of-env-1")), cache.getKafkaClusters("env-1"))
            assertEquals(listOf(cluster("lkc-of-env-2")), cache.getKafkaClusters("env-2"))
            assertEquals(2, control.clusterCalls.get())
        }

        @Test
        fun `should not fetch anything for the cached accessor`() {
            assertNull(cache.getCachedKafkaClusters("env-1"))
            assertEquals(0, control.clusterCalls.get())
        }

        @Test
        fun `should replace the cached clusters on refresh`() {
            control.clusters = { listOf(cluster("lkc-old")) }
            cache.getKafkaClusters("env-1")

            control.clusters = { listOf(cluster("lkc-new")) }
            runBlocking { cache.refreshKafkaClusters("env-1") }

            assertEquals(listOf(cluster("lkc-new")), cache.getCachedKafkaClusters("env-1"))
            assertEquals(listOf(cluster("lkc-new")), cache.getKafkaClusters("env-1"))
            assertEquals(2, control.clusterCalls.get())
        }

        @Test
        fun `should let a refresh that finishes while the first fetch is still running win`() {
            val fetching = CountDownLatch(1)
            val release = CountDownLatch(1)
            val firstFetch = AtomicBoolean(true)
            control.clusters = {
                if (firstFetch.getAndSet(false)) {
                    fetching.countDown()
                    release.await(10, TimeUnit.SECONDS)
                    listOf(cluster("lkc-stale"))
                } else {
                    listOf(cluster("lkc-fresh"))
                }
            }
            val seenByReader = AtomicReference<List<Cluster>>()

            val reader = thread(name = "first-fetch") { seenByReader.set(cache.getKafkaClusters("env-1")) }
            assertTrue(fetching.await(10, TimeUnit.SECONDS), "the first fetch never started")
            runBlocking { cache.refreshKafkaClusters("env-1") }
            release.countDown()
            reader.join(10_000)

            assertEquals(listOf(cluster("lkc-fresh")), seenByReader.get())
            assertEquals(listOf(cluster("lkc-fresh")), cache.getCachedKafkaClusters("env-1"))
        }
    }

    @Nested
    @DisplayName("schema registries")
    inner class SchemaRegistries {

        @Test
        fun `should fetch an existing schema registry once`() {
            control.registries = { registry("lsrc-1") }

            assertEquals(registry("lsrc-1"), cache.getSchemaRegistry("env-1"))
            assertEquals(registry("lsrc-1"), cache.getSchemaRegistry("env-1"))
            assertEquals(registry("lsrc-1"), cache.getCachedSchemaRegistry("env-1"))
            assertEquals(1, control.registryCalls.get())
        }

        @Test
        fun `should remember that an environment has no schema registry`() {
            assertNull(cache.getSchemaRegistry("env-1"))
            assertNull(cache.getSchemaRegistry("env-1"))
            assertNull(cache.getCachedSchemaRegistry("env-1"))

            assertEquals(1, control.registryCalls.get())
        }

        @Test
        fun `should not fetch anything for the cached accessor`() {
            assertNull(cache.getCachedSchemaRegistry("env-1"))
            assertEquals(0, control.registryCalls.get())
        }

        @Test
        fun `should find a schema registry on refresh that was created after none was cached`() {
            assertNull(cache.getSchemaRegistry("env-1"))

            control.registries = { registry("lsrc-new") }
            assertEquals(registry("lsrc-new"), runBlocking { cache.refreshSchemaRegistry("env-1") })

            assertEquals(registry("lsrc-new"), cache.getCachedSchemaRegistry("env-1"))
            assertEquals(registry("lsrc-new"), cache.getSchemaRegistry("env-1"))
            assertEquals(2, control.registryCalls.get())
        }

        @Test
        fun `should notice on refresh that the schema registry is gone`() {
            control.registries = { registry("lsrc-1") }
            cache.getSchemaRegistry("env-1")

            control.registries = { null }
            assertNull(runBlocking { cache.refreshSchemaRegistry("env-1") })

            assertNull(cache.getCachedSchemaRegistry("env-1"))
            assertNull(cache.getSchemaRegistry("env-1"))
            assertEquals(2, control.registryCalls.get())
        }
    }

    @Nested
    @DisplayName("without a fetcher")
    inner class WithoutFetcher {

        private val unconnected = ControlPlaneCache(null, null)

        @AfterEach
        fun dispose() {
            unconnected.dispose()
        }

        @Test
        fun `should neither fetch nor remember anything`() {
            assertEquals(emptyList<Cluster>(), unconnected.getKafkaClusters("env-1"))
            assertNull(unconnected.getCachedKafkaClusters("env-1"))

            assertNull(unconnected.getSchemaRegistry("env-1"))
            assertNull(unconnected.getCachedSchemaRegistry("env-1"))

            assertNull(runBlocking { unconnected.refreshSchemaRegistry("env-1") })
            assertNull(unconnected.getCachedSchemaRegistry("env-1"))
        }
    }

    @Test
    fun `should forget clusters and schema registries on clearCache`() {
        control.clusters = { listOf(cluster("lkc-1")) }
        control.registries = { registry("lsrc-1") }
        cache.getKafkaClusters("env-1")
        cache.getSchemaRegistry("env-1")

        cache.clearCache()

        assertNull(cache.getCachedKafkaClusters("env-1"))
        assertNull(cache.getCachedSchemaRegistry("env-1"))
        cache.getKafkaClusters("env-1")
        cache.getSchemaRegistry("env-1")
        assertEquals(2, control.clusterCalls.get())
        assertEquals(2, control.registryCalls.get())
    }

    @Test
    fun `should not lose entries when many threads fill the cache at once`() {
        val threads = 8
        val environmentsPerThread = 400
        control.clusters = { env -> listOf(cluster("lkc-of-$env")) }
        control.registries = { env -> if (env.hashCode() % 2 == 0) registry("lsrc-of-$env") else null }
        val start = CountDownLatch(1)
        val failures = CopyOnWriteArrayList<Throwable>()

        val workers = (0 until threads).map { t ->
            thread(name = "filler-$t") {
                try {
                    start.await()
                    repeat(environmentsPerThread) { i ->
                        cache.getKafkaClusters("env-$t-$i")
                        cache.getSchemaRegistry("env-$t-$i")
                    }
                } catch (e: Throwable) {
                    failures.add(e)
                }
            }
        }
        start.countDown()
        workers.forEach { it.join(60_000) }

        assertEquals(emptyList<Throwable>(), failures.toList())
        val fetchesWhileFilling = control.clusterCalls.get() to control.registryCalls.get()

        // Everything is cached now, including the environments without a schema registry.
        for (t in 0 until threads) {
            for (i in 0 until environmentsPerThread) {
                val env = "env-$t-$i"
                assertEquals(listOf(cluster("lkc-of-$env")), cache.getCachedKafkaClusters(env), env)
                val expectedRegistry = if (env.hashCode() % 2 == 0) registry("lsrc-of-$env") else null
                assertEquals(expectedRegistry, cache.getCachedSchemaRegistry(env), env)
                assertNotNull(cache.getKafkaClusters(env))
                cache.getSchemaRegistry(env)
            }
        }
        assertEquals(fetchesWhileFilling, control.clusterCalls.get() to control.registryCalls.get())
    }
}
