.PHONY: build

build:
	./gradlew build

.PHONY: coverage
coverage:
	./gradlew :delayedqueue-jvm:koverHtmlReport --no-daemon -x test
	@echo ""
	@echo "Coverage report generated at: jvm/build/reports/kover/html/index.html"
	@echo "To view: open jvm/build/reports/kover/html/index.html"

dependency-updates:
	./gradlew dependencyUpdates \
		--no-parallel \
		-Drevision=release \
		-DoutputFormatter=html \
		--refresh-dependencies && \
		open build/dependencyUpdates/report.html && \
		open delayedqueue-jvm/build/dependencyUpdates/report.html

update-gradle:
	./gradlew wrapper --gradle-version latest

format-scala:
	./sbt scalafmtAll

format-kotlin:
	./gradlew ktfmtFormat

test-scala:
	./sbt "testQuick"

test-scala-watch:
	./sbt "~testQuick"

test-kotlin:
	./gradlew test

test-kotlin-watch:
	./gradlew -t test

test:
	./gradlew test && ./sbt testQuick

check-all:
	./gradlew check && ./sbt check
