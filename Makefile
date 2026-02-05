.PHONY: build

build:
	./gradlew build

.PHONY: coverage
coverage:
	./gradlew :jvm:koverHtmlReport --no-daemon -x test
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
		open jvm/build/dependencyUpdates/report.html

update-gradle:
	./gradlew wrapper --gradle-version latest

test-watch:
	./gradlew -t check
