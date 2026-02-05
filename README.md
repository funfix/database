# delayedqueue

[![Maven Central](https://img.shields.io/maven-central/v/org.funfix/delayedqueue-jvm.svg)](https://search.maven.org/artifact/org.funfix/delayedqueue-jvm)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A Kotlin library for delayed queue implementations on the JVM, designed with maximum Java API compatibility and stability.

## Code Formatting (ktfmt)

This project enforces consistent Kotlin code style using [ktfmt](https://github.com/facebook/ktfmt) via the [com.ncorti.ktfmt.gradle](https://github.com/cortinico/ktfmt-gradle) Gradle plugin. Formatting is automatically checked in CI and can be run locally:

- **Check formatting:**
  ```sh
  ./gradlew ktfmtCheck
  ```
- **Apply formatting:**
  ```sh
  ./gradlew ktfmtFormat
  ```

You can also run these tasks for a specific subproject, e.g.:
  ```sh
  ./gradlew :jvm:ktfmtCheck
  ./gradlew :jvm:ktfmtFormat
  ```

The formatting rules are configured in the shared convention plugin (`buildSrc/src/main/kotlin/delayedqueue.base.gradle.kts`).

## Features

- **Java 21+ Target**: Built for modern JVM applications with virtual threads support
- **Java-Friendly API**: All public APIs use `@JvmRecord` for seamless Java interop
- **Explicit API Mode**: Every public API has explicit visibility modifiers
- **Strict Quality Standards**: 
  - Kotlin progressive mode enabled
  - All warnings treated as errors
  - Detekt with library ruleset
  - Comprehensive documentation requirements
- **Maven Central Ready**: Pre-configured for publishing to Maven Central

## Project Structure

```
delayedqueue/
├── buildSrc/                    # Convention plugins for shared build logic
│   └── src/main/kotlin/
│       ├── delayedqueue.base.gradle.kts      # Base Kotlin + Detekt + Dokka
│       └── delayedqueue.publish.gradle.kts   # Maven Central publishing
├── jvm/                         # JVM subproject (delayedqueue-jvm)
│   └── src/main/kotlin/
└── detekt.yml                   # Detekt configuration with library ruleset
```

## Build Configuration

### Kotlin Compiler Options

- **Target**: JVM 21
- **Language & API**: Kotlin 2.1
- **Progressive Mode**: Enabled
- **All Warnings as Errors**: Enabled
- **Explicit API Mode**: Enabled
- **JVM Options**:
  - `-Xjvm-default=all` - Generate default methods for interfaces
  - `-Xjsr305=strict` - Strict null-safety
  - `-Xemit-jvm-type-annotations` - Emit type annotations to bytecode

### Code Quality

#### Detekt
- **Ruleset**: [Library ruleset](https://detekt.dev/docs/rules/libraries)
- **Configuration**: See `detekt.yml`
- All rules enforce best practices for library development

#### Dokka
- **Format**: Javadoc (for Java compatibility)
- **Generation**: Automated javadoc JAR creation

## Building

```bash
# Build all subprojects
./gradlew build

# Build specific subproject
./gradlew :jvm:build

# Run detekt
./gradlew detekt

# Generate documentation
./gradlew dokkaJavadoc

# Publish to Maven Local
./gradlew publishToMavenLocal
```

## Publishing to Maven Central

1. Configure your credentials in `~/.gradle/gradle.properties`:

```properties
MAVEN_CENTRAL_USERNAME=your-username
MAVEN_CENTRAL_PASSWORD=your-password

# Option 1: Using GPG key file
signing.keyId=your-key-id
signing.password=your-password
signing.secretKeyRingFile=/path/to/secring.gpg

# Option 2: Using in-memory signing (recommended for CI)
ORG_GRADLE_PROJECT_signingInMemoryKey=your-ascii-armored-key
ORG_GRADLE_PROJECT_signingInMemoryKeyPassword=your-password
```

2. Publish:

```bash
# Publish to Maven Central
./gradlew publish

# Or publish all publications
./gradlew publishAllPublicationsToMavenCentralRepository
```

## Development

### Adding a New Subproject

1. Create the directory: `mkdir -p new-subproject/src/main/kotlin`
2. Add to `settings.gradle.kts`: `include("new-subproject")`
3. Create `new-subproject/build.gradle.kts`:

```kotlin
plugins {
    id("delayedqueue.base")
    id("delayedqueue.publish")
}

dependencies {
    // your dependencies
}
```

### Code Style

- Follow Kotlin official code style
- All public APIs must be documented
- Use `@JvmRecord` for data classes exposed to Java
- Prefer explicit visibility modifiers (required by explicit API mode)
- Keep dependencies minimal and well-justified

## Requirements

- **JDK 21+** for building and running
- **Gradle 8.13+** (via wrapper)
- **Kotlin 2.1+**

## License

```
Copyright 2026 Funfix Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
