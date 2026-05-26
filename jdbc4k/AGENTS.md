# Agents Guide: JDBC4K

This is a **Kotlin/JVM** utility library for working with JDBC. It is designed for internal use by other Funfix projects.

## Rules
- **Kotlin-first**: Public APIs are Kotlin idiomatic. No Java interop constraints.
- **Dependencies**: Only `kotlin-stdlib` is allowed for now. No external dependencies unless explicitly approved.
- **TDD**: Write tests before implementation.
- **Binary Compatibility**: Do not change or remove published public members. Add overloads instead.
- **Code Style**: Follow repository conventions (ktfmt, official Kotlin style).

## Testing
- Use Kotlin for internal tests (`src/test/kotlin`).
- Use Java for public API tests (`src/test/java`) if the API is intended for Java consumers.
- Strive for full test coverage.

## Publishing
- This project is published to Maven Central via the `delayedqueue.publish` plugin.
- Version is inherited from the root project (`gradle.properties`).
