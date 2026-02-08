# Agents Guide: Delayed Queue for Scala

This is the **Scala implementation** of the Delayed Queue.
The public API should be idiomatic Scala, leveraging Scala 3 features where appropriate.

## Non-negotiable rules
- Public API is idiomatic Scala: use Scala collections, Option, Either, Try where appropriate.
- Leverage Scala 3 features: extension methods, given/using, top-level definitions, union types.
- Agents MUST practice TDD: write the failing test first, then implement the change.
- Library dependencies should never be added by agents, unless instructed to do so.
- Maintain binary compatibility for published versions.

## API design principles
- Use immutable data structures by default (from `scala.collection.immutable`).
- For error handling, use `Either` or `Option` for pure functions, and use Cats-MTL for error handling in effectful functions (returning `IO` or `F[_]`). Avoid `EitherT[IO, E, A]` or `IO[Either[E, A]]`.
  - Use skill: `cats-mtl-typed-errors`
- Design for composition: small, focused functions/methods.
- Make illegal states unrepresentable with types.

## Scala 3 features
- Use top-level definitions for utilities instead of objects.
- Prefer `extension` methods over implicit classes.
- Use `given`/`using` for contextual abstractions.
- Leverage union types (`A | B`) and intersection types (`A & B`).
- Use opaque type aliases for type safety without runtime overhead.
- Prefer `enum` for ADTs over sealed traits when appropriate.

## Code style / best practices
- Functional programming, pure functions, **required skill**: `cats-effect-io`
- Use 2-space indentation (Scala standard).
- Prefer val over var; minimize mutable state.
- Use meaningful names; avoid abbreviations unless standard (e.g., `acc` for accumulator).
- Keep functions small and focused; extract helpers when needed.
- Use for-comprehensions for sequential operations with flatMap/map.
- Avoid catching `Throwable`; catch `NonFatal(e)` and do something meaningful.
- Use resource management via Cats-Effect `Resource` (required), see skill: `cats-effect-resource`

## Testing
- Practice TDD: write tests before the implementation.
- Strive for full test coverage; tests should be clean and readable.
- Use MUnit
- Test files mirror source structure: `src/main/scala/X.scala` → `src/test/scala/XSpec.scala`.

## Review checklist
- API uses idiomatic Scala types and patterns.
- No leaked implementation details in public signatures.
- Tests cover happy paths and edge cases.
- Code compiles with Scala 3 without warnings.
- Binary compatibility maintained for published versions.

## Build system
- Project uses `sbt` (see `build.sbt` in root).
- This sub-project has non-standard wiring to the Gradle project, so before anything, we need to ensure that the  Gradle dependency was published locally: `sbt publishLocalGradleDependencies`
- Run tests: `sbt test`
- Compile: `sbt compile`
- Format (required): `sbt scalafmtAll`

## References
- Skills:
  - `.agents/skills/cats-effect-io`
  - `.agents/skills/cats-effect-resource`
  - `.agents/skills/cats-mtl-typed-errors`
