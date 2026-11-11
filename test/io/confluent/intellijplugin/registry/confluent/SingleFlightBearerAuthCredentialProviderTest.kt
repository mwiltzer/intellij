package io.confluent.intellijplugin.registry.confluent

import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.URL
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class SingleFlightBearerAuthCredentialProviderTest {

    private val url = URL("http://localhost/subjects")

    /** Stand-in for the OAuth provider of the Schema Registry client. [tokenForCall] gets the number of the call. */
    private class FakeProvider(private val tokenForCall: (call: Int) -> String?) : BearerAuthCredentialProvider {
        val calls = AtomicInteger()
        var configured: Map<String, *>? = null
        var closed = false

        override fun alias() = "FAKE"
        override fun getBearerToken(url: URL?): String? = tokenForCall(calls.incrementAndGet())
        override fun getTargetSchemaRegistry() = "lsrc-1"
        override fun getTargetIdentityPoolId() = "pool-1"
        override fun configure(configs: MutableMap<String, *>?) {
            configured = configs
        }

        override fun close() {
            closed = true
        }
    }

    /** Threads that ask for a token at the same time, and a way for the one that gets to run to see the others wait. */
    private class Burst(private val callers: Int, private val call: () -> String?) {
        private val registered = CountDownLatch(callers)
        private val threads = CopyOnWriteArrayList<Thread>()
        private val results = ConcurrentLinkedQueue<Result<String?>>()

        fun run(): List<Result<String?>> {
            val all = List(callers) {
                Thread {
                    threads += Thread.currentThread()
                    registered.countDown()
                    registered.await()
                    results += runCatching(call)
                }
            }
            all.forEach { it.start() }
            all.forEach { it.join(TIMEOUT_MS) }
            check(all.none { it.isAlive }) { "Callers are still running" }
            return results.toList()
        }

        /** Blocks until every other caller waits in the single-flight for the outcome of the caller that runs this. */
        fun awaitOthersWaiting() {
            registered.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            val others = threads.filter { it !== Thread.currentThread() }
            val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS)
            while (!others.all { it.isWaitingInSingleFlight() }) {
                check(System.nanoTime() < deadline) { "The other callers do not wait: ${others.map { it.state }}" }
                Thread.sleep(1)
            }
        }
    }

    @Test
    fun `should request the token once for callers that ask at the same time`() {
        lateinit var burst: Burst
        val provider = FakeProvider { call ->
            burst.awaitOthersWaiting()
            "token-$call"
        }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)
        burst = Burst(CALLERS) { wrapper.getBearerToken(url) }

        val results = burst.run()

        assertEquals(1, provider.calls.get())
        assertEquals(List(CALLERS) { "token-1" }, results.map { it.getOrThrow() })
    }

    @Test
    fun `should fail all callers that ask at the same time when the token request fails`() {
        lateinit var burst: Burst
        val provider = FakeProvider {
            burst.awaitOthersWaiting()
            throw IllegalStateException("identity provider is down")
        }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)
        burst = Burst(CALLERS) { wrapper.getBearerToken(url) }

        val results = burst.run()

        assertEquals(1, provider.calls.get())
        assertEquals(List(CALLERS) { "identity provider is down" }, results.map { it.exceptionOrNull()?.message })
    }

    @Test
    fun `should request again once the previous request has finished`() {
        val provider = FakeProvider { call -> "token-$call" }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)

        val tokens = List(3) { wrapper.getBearerToken(url) }

        // Remembering the token is up to the wrapped provider, the wrapper only merges requests that overlap.
        assertEquals(listOf("token-1", "token-2", "token-3"), tokens)
    }

    @Test
    fun `should request again after a failure`() {
        val provider = FakeProvider { call -> if (call == 1) error("identity provider is down") else "token-$call" }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)

        val failure = runCatching { wrapper.getBearerToken(url) }
        val token = wrapper.getBearerToken(url)

        assertEquals("identity provider is down", failure.exceptionOrNull()?.message)
        assertEquals("token-2", token)
    }

    @Test
    fun `should not fail the waiting callers when the caller that requested the token was cancelled`() {
        lateinit var burst: Burst
        val provider = FakeProvider { call ->
            if (call == 1) {
                burst.awaitOthersWaiting()
                throw RuntimeException("cancelled", InterruptedException())
            }
            "token"
        }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)
        burst = Burst(CALLERS) { wrapper.getBearerToken(url) }

        val results = burst.run()

        assertEquals(1, results.count { it.isFailure }, "Only the cancelled caller fails")
        assertEquals(List(CALLERS - 1) { "token" }, results.filter { it.isSuccess }.map { it.getOrThrow() })
        assertTrue(provider.calls.get() >= 2)
    }

    @Test
    fun `should stop waiting and keep the interruption when interrupted while another caller requests the token`() {
        val requesting = CountDownLatch(1)
        val release = CountDownLatch(1)
        val provider = FakeProvider {
            requesting.countDown()
            release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            "token"
        }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)
        val requester = Thread { wrapper.getBearerToken(url) }.apply { start() }
        requesting.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)

        val error = AtomicReference<Throwable>()
        val stillInterrupted = AtomicReference<Boolean>()
        val waiter = Thread {
            try {
                wrapper.getBearerToken(url)
            } catch (t: Throwable) {
                error.set(t)
                stillInterrupted.set(Thread.currentThread().isInterrupted)
            }
        }.apply { start() }
        while (!waiter.isWaitingInSingleFlight()) Thread.sleep(1)

        waiter.interrupt()
        waiter.join(TIMEOUT_MS)
        release.countDown()
        requester.join(TIMEOUT_MS)

        assertTrue(error.get() is SchemaRegistryOauthTokenRetrieverException, "Got ${error.get()}")
        assertTrue(stillInterrupted.get() == true)
        assertEquals(1, provider.calls.get())
    }

    @Test
    fun `should leave everything but the token to the wrapped provider`() {
        val provider = FakeProvider { "token" }
        val wrapper = SingleFlightBearerAuthCredentialProvider(provider)

        wrapper.configure(mutableMapOf("key" to "value"))
        wrapper.close()

        assertEquals("FAKE", wrapper.alias())
        assertEquals("lsrc-1", wrapper.targetSchemaRegistry)
        assertEquals("pool-1", wrapper.targetIdentityPoolId)
        assertEquals(mapOf("key" to "value"), provider.configured)
        assertTrue(provider.closed)
    }

    private companion object {
        const val CALLERS = 8
        const val TIMEOUT_MS = 10_000L

        // The thread state alone is no proof: a thread that is released from the start latch still reports WAITING
        // for a moment, before it asked for the token.
        fun Thread.isWaitingInSingleFlight() =
            state == Thread.State.WAITING && stackTrace.any { it.className.contains("BlockingSingleFlight") }
    }
}
