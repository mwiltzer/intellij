package io.confluent.intellijplugin.registry.common

import io.confluent.intellijplugin.core.util.toPresentableText
import io.confluent.intellijplugin.util.KafkaMessagesBundle
import java.security.cert.CertPathBuilderException
import java.security.cert.CertPathValidatorException
import java.security.cert.CertificateException

/**
 * Builds the text shown when the Schema Registry cannot be reached.
 *
 * A bare "registry is not available" left the user with the stack trace as the only place to learn *why*,
 * so the text carries the reason of the failure, plus a hint for the failures that have a well-known cure.
 */
object RegistryConnectionFailure {

    private const val MAX_REASON_LENGTH = 300
    private const val MAX_CAUSE_DEPTH = 15
    private const val OAUTHBEARER_PACKAGE = "org.apache.kafka.common.security.oauthbearer"
    private const val SCHEMA_REGISTRY_OAUTH_PACKAGE = ".bearerauth.oauth."

    private val whitespace = Regex("\\s+")

    fun describe(failure: Throwable): String {
        val reason = reasonOf(failure)
        val hint = hintFor(failure)
        val text = KafkaMessagesBundle.message("connection.kafka.registry.is.not.available.reason", reason)
        return if (hint == null) text else "$text $hint"
    }

    internal fun reasonOf(failure: Throwable): String {
        val reason = failure.toPresentableText().replace(whitespace, " ").trim()
        return if (reason.length <= MAX_REASON_LENGTH) reason else reason.take(MAX_REASON_LENGTH).trimEnd() + "…"
    }

    internal fun hintFor(failure: Throwable): String? {
        val causes = causesOf(failure)
        return when {
            causes.any(::isCertificateProblem) -> KafkaMessagesBundle.message("connection.kafka.registry.hint.tls")
            causes.any(::isOAuthProblem) -> KafkaMessagesBundle.message("connection.kafka.registry.hint.oauth")
            else -> null
        }
    }

    // The chain is bounded so a self-referencing cause can't loop forever.
    private fun causesOf(failure: Throwable): List<Throwable> =
        generateSequence(failure) { it.cause?.takeIf { cause -> cause !== it } }.take(MAX_CAUSE_DEPTH).toList()

    private fun isCertificateProblem(t: Throwable) =
        t is CertificateException || t is CertPathBuilderException || t is CertPathValidatorException

    private fun isOAuthProblem(t: Throwable): Boolean {
        val name = t.javaClass.name
        return name.startsWith(OAUTHBEARER_PACKAGE) || name.contains(SCHEMA_REGISTRY_OAUTH_PACKAGE)
    }
}
