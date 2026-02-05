# Kotlin Java Library Design - Reference

Concise reference for Kotlin APIs intended for Java consumers.

Sources:
- https://kotlinlang.org/docs/java-to-kotlin-interop.html
- https://kotlinlang.org/docs/jvm-records.html
- https://kotlinlang.org/docs/api-guidelines-backward-compatibility.html
- https://developer.android.com/kotlin/interop
- https://proandroiddev.com/everything-about-jvmfield-jvmoverloads-jvmname-and-jvmstatic-annotations-in-kotlin-158383081cb8


## Table of Contents
- [Interop goals](#interop-goals)
- [Java-friendly API design](#java-friendly-api-design)
- [JVM annotations and patterns](#jvm-annotations-and-patterns)
- [JVM records](#jvm-records)
- [Backward compatibility rules](#backward-compatibility-rules)
- [Checklist](#checklist)
- [Test prompts](#test-prompts)

## Interop goals
- Make the Java surface explicit and stable: names, overloads, and nullability should be unsurprising.
- Prefer predictable JVM signatures over Kotlin-only conveniences for public APIs.
- Verify with a Java call site before publishing.

## Java-friendly API design
- Favor simple constructors or factory methods with overloads for optional parameters.
- Expose top-level utilities with a stable class name via `@file:JvmName`.
- Consider `@file:JvmMultifileClass` to merge multiple files into one Java facade.
- Avoid forcing Java callers to use Kotlin-only idioms (e.g., extension-only entry points or default args without overloads).
- Keep nullability explicit; avoid platform types in public signatures.
- Use `@Throws` to declare checked exceptions for Java callers.
- Avoid returning shared mutable collections; return defensive copies or unmodifiable views.

### Example: stable class name for top-level functions

```kotlin
@file:JvmName("UserIds")

package com.example.ids

fun parseUserId(raw: String): Long = raw.toLong()
```

Java call site:

```java
long id = UserIds.parseUserId("42");
```

## JVM annotations and patterns

### @JvmOverloads
Use when Java needs overloads for default parameters.

```kotlin
class Client @JvmOverloads constructor(
  val endpoint: String,
  val timeoutMillis: Long = 2_000,
  val retries: Int = 1
)

@JvmOverloads
fun connect(host: String, port: Int = 443, secure: Boolean = true): String =
  "$host:$port?secure=$secure"
```

Java call site:

```java
Client client = new Client("https://api.example.com");
String url = connect("example.com");
```

Guidance:
- Use `@JvmOverloads` only when the defaults are stable and unambiguous.
- If the Java behavior should differ from Kotlin defaults, write explicit overloads instead.
- When evolving APIs, add manual overloads to preserve binary compatibility.

### @JvmStatic
Use to expose companion/object members as Java `static` methods or fields.

```kotlin
class Ids private constructor() {
  companion object {
    @JvmStatic
    fun newId(): String = java.util.UUID.randomUUID().toString()
  }
}
```

Java call site:

```java
String id = Ids.newId();
```

### @JvmField
Expose a field directly to Java without getters/setters.

```kotlin
object Defaults {
  @JvmField val MAX_RETRIES: Int = 3
  const val API_VERSION: String = "v1"
}
```

Java call site:

```java
int retries = Defaults.MAX_RETRIES;
String v = Defaults.API_VERSION;
```

Guidance:
- Prefer for constants or immutable fields. Once exposed as a field, changing it to a property is binary-incompatible.
- For companion object constants, `@JvmField` exposes a static field; `@JvmStatic` does not.

### @JvmName
Use to avoid JVM signature clashes or to provide a stable Java name.

```kotlin
fun String.lengthInChars(): Int = length

@JvmName("lengthInUtf8")
fun String.lengthInBytes(): Int = toByteArray(Charsets.UTF_8).size
```

Java call site:

```java
int utf8 = KotlinInterop.lengthInUtf8("hi");
```

Guidance:
- Use `@JvmName` on overloads when erased signatures would collide.
- Prefer clear Java naming, especially for top-level or companion APIs.

### @JvmMultifileClass
Use with `@file:JvmName` to merge multiple Kotlin files into one Java facade class.

```kotlin
@file:JvmName("HttpUtils")
@file:JvmMultifileClass

package com.example.http

fun parseHeader(value: String): String = value.trim()
```

Guidance:
- Ensure all files that participate share the same package and `@file:JvmName`.

### @Throws
Use to expose checked exceptions to Java.

```kotlin
@Throws(java.io.IOException::class)
fun writeConfig(path: String) { /* ... */ }
```

Java call site:

```java
try { writeConfig("/tmp/app.conf"); }
catch (IOException e) { /* ... */ }
```

### Wildcards for generics
Kotlin variance maps to Java wildcards on parameters but not on return types.

```kotlin
fun accept(box: Box<out Base>) {}
fun provide(): Box<Derived> = Box(Derived())

fun acceptNoWildcards(box: Box<@JvmSuppressWildcards Base>) {}
fun provideWildcards(): Box<@JvmWildcard Derived> = Box(Derived())
```

Guidance:
- Use `@JvmSuppressWildcards` to simplify Java signatures.
- Use `@JvmWildcard` when Java needs a wildcard on a return type.

### Companion functions and fields
- Companion object functions meant for Java should be `@JvmStatic`.
- Companion constants that are not `const` should be `@JvmField` to be true static fields.

## JVM records
Use `@JvmRecord` on a Kotlin `data class` to emit a Java record.

```kotlin
@JvmRecord
data class UserId(val value: String)
```

Guidance:
- Records require a JVM target that supports them (Java 16+).
- Record components map to the primary constructor `val` properties.
- Do not add mutable state; records are intended to be transparent carriers.
- Applying `@JvmRecord` to an existing class is not binary compatible.

## Backward compatibility rules
- Explicitly declare public return types; inferred return types can change binary signatures.
- Do not change or remove public API members once published.
- Avoid renaming packages, classes, or public members; prefer adding new members and deprecating old ones.
- Changing the type, nullability, or generic arity of a public API is binary-incompatible.
- Changing a property to a function (or vice versa) breaks Java callers.
- Adding parameters to public functions breaks Java call sites unless you keep overloads.
- Adding default arguments to existing functions breaks binary compatibility unless you keep manual overloads.
- Changing default argument values changes behavior at existing Kotlin call sites; document it and consider a new overload.
- Adding an overload can introduce ambiguity for Kotlin callers; verify call sites.
- Avoid data classes in public API when you expect to evolve the constructor or `copy` signature.
- Treat `@PublishedApi` members as public API for compatibility purposes.
- Use a deprecation cycle (`WARNING` -> `ERROR` -> `HIDDEN`) before removal.

## Kotlin for Java consumers (Android interop highlights)
- Always annotate files with top-level declarations using `@file:JvmName`.
- Prefer `fun interface` for callbacks meant to be used as lambdas in Java and Kotlin.
- Avoid function types that return `Unit` in public APIs; they require `Unit.INSTANCE` in Java.
- Avoid `Nothing` in public generic signatures; Java sees raw types.
- Use `@Throws` for checked exceptions and document runtime exceptions in KDoc.
- Return defensive copies or unmodifiable views for read-only collections.
- Use `@JvmOverloads` for default arguments and validate the generated overload set.

## Checklist
- Public API names are stable and Java-friendly.
- Java call-site examples compile for every public entry point.
- `@JvmOverloads`, `@JvmStatic`, `@JvmField`, and `@JvmName` are used intentionally.
- No public API changes that break binary compatibility.
- Defaults and overloads are documented for Java callers.

## Test prompts
- "Design a Kotlin API for a Java SDK with optional args and stable overloads."
- "Make this Kotlin utility Java-friendly using JVM annotations and show Java call sites."
- "Audit this Kotlin public API for backward-compatibility risks and propose fixes."
- "Model a value object as a Java record using Kotlin; list the constraints."
