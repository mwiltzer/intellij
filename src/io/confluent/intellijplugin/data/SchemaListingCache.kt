package io.confluent.intellijplugin.data

/**
 * Everything the registry listed lately, for the tool window and the editors: the listing of all schemas, which the
 * editors need, and the listings of the schemas that start with a prefix, which the filter of the tool window asks for
 * and which cost a fraction of listing them all.
 *
 * It decides which of them serves a request, and when the registry has to be asked.
 */
internal class SchemaListingCache(
    private val all: ExpiringSingleFlight<SchemaListing>,
    private val prefixes: PrefixListings
) {

    /** Everything that was listed, whether it has all the schemas or only those of a prefix. */
    fun listed(): List<SchemaListing> = listOfNotNull(all.peek()) + prefixes.cached()

    /**
     * What was listed lately that has all the schemas a filter that asks for [prefix] lets through, if there is any.
     * `null` is a filter that is no prefix, for which only a listing of all schemas does. Never asks the registry.
     */
    fun cachedFor(prefix: String?): SchemaListing? = SchemaListings.bestFor(prefix, listed())

    /** Whether the registry has the schema [name], as far as what was listed tells; `null` if that does not. */
    fun knownToHave(name: String): Boolean? = SchemaListings.knowingAbout(name, listed())?.contains(name)

    /**
     * The listing to make the list of the tool window out of for a filter that asks for the schemas that start with
     * [prefix]; all the schemas if it is `null`, or empty, a filter that is still being typed.
     *
     * A registry that can list by prefix is asked for those schemas only. A listing of all the schemas that is there
     * already, e.g. one the editors made, has them all, so it is taken, unless the registry is to be listed anew with
     * [refresh], which lists it for the prefix only. A registry that can not list by prefix lists all the schemas.
     */
    suspend fun listingFor(prefix: String?, refresh: Boolean): SchemaListing {
        if (!prefix.isNullOrEmpty()) {
            if (!refresh) all.peek()?.let { return it }
            prefixes.get(prefix, refresh)?.let { return it }
        }
        return if (refresh) all.refresh() else all.get()
    }

    /**
     * Whether the registry has the schema [name]: what was listed tells, or else the registry is asked for the schemas
     * that start with the name, which is the cheapest request there is for it.
     */
    suspend fun has(name: String): Boolean = knownToHave(name) ?: (name in listingFor(prefix = name, refresh = false))

    /**
     * The names of the schemas that start with [prefix], as current as one cheap request tells: the registry is asked for
     * those schemas only, also when all the schemas were listed lately. That listing can be minutes old, and the schema
     * of a topic that was registered since is what an editor looks for. What the registry answers is kept for a minute,
     * see [PrefixListings].
     *
     * A registry that can not list by prefix has only the listing of all schemas to tell, if one was made lately.
     * `null` if there is none, or [prefix] is empty, which asks for all schemas: listing all of them is the only way.
     */
    suspend fun namesStartingWith(prefix: String): List<String>? {
        if (prefix.isEmpty()) return null
        val listing = prefixes.get(prefix) ?: all.peekFresh() ?: return null
        // A listing for a shorter prefix, or of all schemas, has others as well.
        return listing.names.filter { it.startsWith(prefix) }
    }

    /** Forgets everything that was listed, e.g. after the schemas of the registry were changed. */
    fun clear() {
        all.invalidate()
        prefixes.clear()
    }
}
