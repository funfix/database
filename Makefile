.PHONY: build

build:
	./gradlew build

dependency-updates:
	./gradlew dependencyUpdates \
		-Drevision=release \
		-DoutputFormatter=html \
		--refresh-dependencies && \
		open build/dependencyUpdates/report.html

update-gradle:
	./gradlew wrapper --gradle-version latest

test-watch:
	./gradlew -t check
