package io.confluent.intellijplugin.data

import kotlinx.coroutines.CoroutineScope

/**
 * How long the names of all the schemas are kept when the registry lists by prefix. Listing a big registry takes
 * seconds, and the editors ask it for the schemas of their topic anyway, so names that are older are no harm.
 */
internal const val SCHEMA_NAMES_TTL_MS = 5 * 60_000L

/** How long they are kept for a registry that can not list by prefix, or was not shown to. */
internal const val SCHEMA_NAMES_SHORT_TTL_MS = 60_000L

/**
 * Lists the names of all the schemas of a registry once for everyone who asks at the same time, and keeps them.
 *
 * A schema that is registered after the names were listed turns up in the editors, for the topic that they are opened
 * for, when the registry can list by prefix: that is a cheap request they make every time. Then the names are kept for
 * [SCHEMA_NAMES_TTL_MS], and until the [prefixes] have shown that the registry can, for [SCHEMA_NAMES_SHORT_TTL_MS].
 * A registry that can not has only these names to tell of a new schema, so they must not be old.
 */
internal fun schemaNamesFlight(
    scope: CoroutineScope,
    prefixes: PrefixListings,
    clock: () -> Long = System::currentTimeMillis,
    load: suspend () -> SchemaListing
) = ExpiringSingleFlight(
    scope,
    ttlMillis = { if (prefixes.canListByPrefix) SCHEMA_NAMES_TTL_MS else SCHEMA_NAMES_SHORT_TTL_MS },
    clock = clock,
    load = load
)
