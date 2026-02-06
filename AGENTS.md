# Agents Guide

This repository ships a Delayed Queue for Java developers, implemented in Kotlin.
All public APIs must look and feel like a Java library. The `kotlin-java-library`
skill is the rule of law for any public surface changes.

## Non-negotiable rules
- Public API is Java-first: no Kotlin-only surface features or Kotlin stdlib types.
- Keep nullability explicit and stable; avoid platform types in public signatures.
- Do not change or remove published public members; add overloads instead.
- Use JVM interop annotations deliberately to shape Java call sites.
- Verify every public entry point with a Java call-site example.
- Agents MUST practice TDD: write the failing test first, then implement the change.
- Library dependencies should never be added by agents, unless explicitly instructed.

## Public API constraints (Java consumers)
- Use Java types in signatures: `java.util.List/Map/Set`, `java.time.*`,
  `java.util.concurrent.*`, `java.util.Optional`, `java.io.*`.
- Avoid Kotlin types in public API: `kotlin.collections.*`, `kotlin.Result`,
  `kotlin.Unit`, `kotlin.Pair/Triple`, `kotlin.time.*`, `kotlinx.*`.
- Avoid default arguments without `@JvmOverloads` or explicit overloads.
- Avoid extension-only entry points; expose top-level or static members.
- Use `@Throws` for checked exceptions expected by Java callers.
- Return defensive copies or unmodifiable views for collections.

## JVM interop patterns
- `@JvmOverloads` for stable default arguments; prefer explicit overloads when
  behavior differs between overloads.
- `@JvmStatic` on companion/object functions that should be `static` in Java.
- `@JvmField` only for true constants or immutable fields.
- `@JvmName` to avoid signature clashes or provide stable Java names.
- `@file:JvmName` for top-level utilities, optionally `@file:JvmMultifileClass`.

## Binary compatibility
- Declare explicit public return types; avoid inferred public signatures.
- Do not change types, nullability, or parameter order of public members.
- Add new overloads instead of changing existing ones.
- Use a deprecation cycle before removal.

## Code style / best practices

- NEVER catch `Throwable`, you're only allowed to catch `Exception`

## Testing

- Practice TDD: write tests before the implementation.
- Projects strives for full test coverage. Tests have to be clean and easy to read.
- All public tests go into `./src/test/java`, built in Java.
- All tests for internals go into `./src/test/kotlin`, built in Kotlin

## Review checklist
- Java call sites compile for all public constructors and methods.
- No Kotlin stdlib types exposed in public signatures.
- Default args are covered by overloads for Java.
- Exceptions are declared with `@Throws` where relevant.
- Tests targeting the public API should be Java tests.

## References
- Skill: `.claude/skills/kotlin-java-library`
- Reference: `.claude/skills/kotlin-java-library/references/kotlin-java-library.md`
