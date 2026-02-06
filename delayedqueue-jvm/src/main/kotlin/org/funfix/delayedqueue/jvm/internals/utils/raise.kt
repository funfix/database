package org.funfix.delayedqueue.jvm.internals.utils

/**
 * A context parameter type that enables compile-time tracking of checked exceptions.
 *
 * ## Purpose
 *
 * The `Raise` context parameter allows functions to declare what checked exceptions they can throw
 * in a way that the Kotlin type system can track. This is superior to traditional exception
 * handling because:
 * 1. **Compile-time safety**: The compiler ensures all exception paths are handled
 * 2. **Explicit exception flow**: The type system documents exception propagation
 * 3. **Java interop**: Maps cleanly to `@Throws` declarations for Java consumers
 *
 * ## Usage Pattern
 *
 * Functions that can raise exceptions declare a `context(Raise<E>)` parameter:
 * ```kotlin
 * context(_: Raise<SQLException>)
 * fun queryDatabase(): ResultSet {
 *     // Can throw SQLException
 *     return connection.executeQuery(sql)
 * }
 *
 * context(_: Raise<SQLException>, _: Raise<InterruptedException>)
 * fun complexOperation() {
 *     // Can throw both SQLException and InterruptedException
 *     queryDatabase()  // Automatically gets the Raise<SQLException> context
 * }
 * ```
 *
 * ## Architecture in DelayedQueue
 *
 * The library uses a layered exception handling approach:
 * 1. **Internal methods** declare fine-grained `context(Raise<SQLException>,
 *    Raise<InterruptedException>)`
 *     - These are the methods that directly call `database.withConnection/withTransaction`
 *     - The type system tracks that SQLException can be raised
 * 2. **Retry wrapper** (`withRetries`/`withDbRetries`) has
 *    `context(Raise<ResourceUnavailableException>, Raise<InterruptedException>)`
 *     - Catches SQLException and TimeoutException
 *     - Wraps them into ResourceUnavailableException after retries are exhausted
 *     - Type system knows it raises ResourceUnavailableException, not SQLException
 * 3. **Public API methods** use `unsafeSneakyRaises` ONLY at the boundary
 *     - Declared with `@Throws(ResourceUnavailableException::class, InterruptedException::class)`
 *     - Call `unsafeSneakyRaises { withRetries { internalMethod() } }`
 *     - This suppresses the Raise context into the @Throws annotation for Java
 *
 * ## Contract
 * - **NEVER use `unsafeSneakyRaises` in internal implementations**
 * - It defeats the purpose of Raise by hiding exception flow from the type system
 * - Only use at public API boundaries where `@Throws` declarations exist
 * - Only use in tests where exception tracking is not needed
 *
 * @param E The exception type that can be raised
 */
@JvmInline
internal value class Raise<in E : Exception> private constructor(val fake: Nothing? = null) {
    companion object {
        val _PRIVATE_AND_UNSAFE: Raise<Exception> = Raise()
    }
}

/**
 * Raises an exception within a Raise context.
 *
 * This function can only be called when a `Raise<E>` context is available, ensuring compile-time
 * tracking of exception types.
 *
 * @param exception The exception to raise
 * @return Never returns (always throws)
 * @throws E Always throws the provided exception
 */
context(_: Raise<E>)
internal inline fun <reified E : Exception> raise(exception: E): Nothing = throw exception

/**
 * **DANGER: Only use at public API boundaries or in tests!**
 *
 * Provides a `Raise<Exception>` context to a block, bypassing compile-time exception tracking.
 *
 * ## When to use
 * 1. **Public API methods with @Throws declarations** ```kotlin
 *
 * @param block The code to execute with an unsafe Raise context
 * @return The result of executing the block
 *     @Throws(ResourceUnavailableException::class, InterruptedException::class) override fun
 *       poll(): AckEnvelope<A> = unsafeSneakyRaises { withRetries { internalPoll() } } ``` The
 *       `@Throws` annotation serves as the Java contract, and `unsafeSneakyRaises` suppresses the
 *       Raise context at the boundary.
 * 2. **Tests where exception tracking is not needed**
 *
 * ## When NOT to use
 * - **NEVER in internal implementations** - defeats the purpose of Raise
 * - **NEVER when you can use proper Raise context** - always prefer explicit context
 * - **NEVER to hide exception handling** - the type system should track exceptions
 *
 * ## Why it exists
 *
 * Kotlin's context receivers are not yet visible to Java, so we need a way to bridge between
 * Kotlin's Raise context and Java's `@Throws` declarations at the public API.
 */
internal inline fun <T> unsafeSneakyRaises(
    block:
        context(Raise<Exception>)
        () -> T
): T = block(Raise._PRIVATE_AND_UNSAFE)
