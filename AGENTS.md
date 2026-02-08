# Agents Guide

This is a multi-project repository containing libraries and tools built for different platforms and languages.

## Navigation

**Each project has its own AGENTS.md file with language-specific rules and best practices.**

Before working on code, identify which project you're in and consult its guide:

| Project                   | Path                  | Description                                     | Guide                                     |
| ------------------------- | --------------------- | ----------------------------------------------- | ----------------------------------------- |
| **Delayed Queue (JVM)**   | `delayedqueue-jvm/`   | Kotlin implementation targeting Java developers | [AGENTS.md](delayedqueue-jvm/AGENTS.md)   |
| **Delayed Queue (Scala)** | `delayedqueue-scala/` | Scala 3 implementation with functional APIs     | [AGENTS.md](delayedqueue-scala/AGENTS.md) |

## How to Work in This Repository

1. **Identify the project** you're working in by checking the file path
2. **Read that project's AGENTS.md** for language-specific rules and constraints
3. **Follow the project-specific guidelines** for code style, testing, and API design

## Universal Rules (All Projects)

- **TDD is mandatory**: Write the failing test first, then implement the change.
- **No unauthorized dependencies**: Library dependencies should never be added by agents unless explicitly instructed.
- **Binary compatibility**: Do not change or remove published public members; add overloads instead.
- **Code quality**: Maintain full test coverage; tests must be clean and easy to read.

## References

- Project documentation: `docs/`
