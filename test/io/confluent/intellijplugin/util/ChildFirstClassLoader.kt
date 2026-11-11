package io.confluent.intellijplugin.util

/**
 * A class loader that loads the classes [isFresh] says yes to again, from the class files the [parent] loads them from.
 * Those classes are not the ones of the parent, and the static state of the classes of the parent is not theirs. All
 * other classes come from the parent.
 *
 * The code source of a class can not tell where its file is: the class loader of the IDE does not give one. The parent
 * serves the file instead.
 */
class ChildFirstClassLoader(parent: ClassLoader, private val isFresh: (className: String) -> Boolean) : ClassLoader(parent) {

    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        synchronized(getClassLoadingLock(name)) {
            if (!isFresh(name)) return super.loadClass(name, resolve)

            val loaded = findLoadedClass(name) ?: define(name)
            if (resolve) resolveClass(loaded)
            return loaded
        }
    }

    private fun define(name: String): Class<*> {
        val bytes = parent.getResourceAsStream(name.replace('.', '/') + ".class")?.use { it.readAllBytes() }
            ?: throw ClassNotFoundException(name)
        return defineClass(name, bytes, 0, bytes.size)
    }
}
