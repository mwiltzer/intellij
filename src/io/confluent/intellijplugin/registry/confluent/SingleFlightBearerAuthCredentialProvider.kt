package io.confluent.intellijplugin.registry.confluent

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions.SchemaRegistryOauthTokenRetrieverException
import java.net.URL
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

/**
 * Runs a blocking operation for one caller and hands its outcome, a failure included, to the callers that
 * arrive while it runs, instead of running it once more for each of them.
 *
 * Unlike a lock this does not line the callers up: when the operation fails after a long timeout, they all
 * fail together rather than one timeout after the other.
 */
internal class BlockingSingleFlight<T> {

    private sealed interface Outcome<out T> {
        class Done<T>(val value: T) : Outcome<T>
        class Failed(val error: Throwable) : Outcome<Nothing>

        /** The caller that ran the operation got cancelled, which tells nothing about the operation itself. */
        data object Abandoned : Outcome<Nothing>
    }

    private val running = AtomicReference<CompletableFuture<Outcome<T>>?>()

    /** @throws InterruptedException when the thread is interrupted while it waits for the outcome of another caller */
    @Throws(InterruptedException::class)
    fun run(operation: () -> T): T {
        while (true) {
            val shared = running.get()
            if (shared != null) {
                when (val outcome = shared.get()) {
                    is Outcome.Done -> return outcome.value
                    is Outcome.Failed -> throw outcome.error
                    Outcome.Abandoned -> {
                        // The one who ran it is gone, so one of the waiting callers has to run it now.
                        running.compareAndSet(shared, null)
                        continue
                    }
                }
            }

            val mine = CompletableFuture<Outcome<T>>()
            if (!running.compareAndSet(null, mine)) continue

            try {
                val value = operation()
                mine.complete(Outcome.Done(value))
                return value
            } catch (t: Throwable) {
                val abandoned = Thread.currentThread().isInterrupted || t.isCausedByInterruption()
                mine.complete(if (abandoned) Outcome.Abandoned else Outcome.Failed(t))
                throw t
            } finally {
                running.compareAndSet(mine, null)
            }
        }
    }

    private fun Throwable.isCausedByInterruption(): Boolean =
        generateSequence(this) { it.cause }
            .take(MAX_CAUSE_DEPTH)
            .any { it is InterruptedException || it is ClosedByInterruptException }

    private companion object {
        const val MAX_CAUSE_DEPTH = 16
    }
}

/**
 * Lets concurrent [getBearerToken] calls share a single token request.
 *
 * The Schema Registry client keeps the token it got, but when there is none or it expired, every caller
 * that notices fetches a token of its own, without any locking. Opening an editor, refreshing the tool window and
 * detecting schemas easily start a dozen of registry calls at once, so that many token requests hit the identity
 * provider, which may rate limit them.
 */
internal class SingleFlightBearerAuthCredentialProvider(
    private val delegate: BearerAuthCredentialProvider
) : BearerAuthCredentialProvider {

    private val tokenRequests = BlockingSingleFlight<String?>()

    override fun alias(): String? = delegate.alias()

    override fun getBearerToken(url: URL?): String? = try {
        tokenRequests.run { delegate.getBearerToken(url) }
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        throw SchemaRegistryOauthTokenRetrieverException("Interrupted while waiting for the OAuth token", e)
    }

    override fun getTargetSchemaRegistry(): String? = delegate.targetSchemaRegistry

    override fun getTargetIdentityPoolId(): String? = delegate.targetIdentityPoolId

    override fun configure(configs: MutableMap<String, *>?) = delegate.configure(configs)

    override fun close() = delegate.close()
}

/**
 * A [RestService] that makes its OAuth bearer provider request tokens single-flight.
 *
 * [RestService.configure] creates the provider from the settings and hands it to [setBearerAuthCredentialProvider],
 * the only place where it can be put behind a [SingleFlightBearerAuthCredentialProvider]: the service does not
 * give the provider away, and it is replaced each time the service gets configured.
 */
internal class SingleFlightTokenRestService(url: String) : RestService(url) {

    override fun setBearerAuthCredentialProvider(provider: BearerAuthCredentialProvider?) {
        val requestsTokens = provider != null &&
                provider !is SingleFlightBearerAuthCredentialProvider &&
                provider.alias() == OAUTH_BEARER_ALIAS
        super.setBearerAuthCredentialProvider(
            if (requestsTokens) SingleFlightBearerAuthCredentialProvider(provider!!) else provider
        )
    }

    private companion object {
        /** Alias of the provider that requests its tokens from an OAuth identity provider. */
        const val OAUTH_BEARER_ALIAS = "OAUTHBEARER"
    }
}
