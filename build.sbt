import java.io.FileInputStream
import java.util.Properties

val publishLocalGradleDependencies =
  taskKey[Unit]("Builds and publishes gradle dependencies")

val props =
  settingKey[Properties]("Main project properties")

inThisBuild(
  Seq(
    organization := "org.funfix",
    scalaVersion := "3.8.1",
    scalacOptions ++= Seq(
      "-no-indent",
      "-Yexplicit-nulls",
    ),
    // ---
    // Settings for dealing with the local Gradle-assembled artifacts
    // Also see: publishLocalGradleDependencies
    resolvers ++= Seq(Resolver.mavenLocal),
    props := {
      val projectProperties = new Properties()
      val rootDir = (ThisBuild / baseDirectory).value
      val fis = new FileInputStream(s"$rootDir/gradle.properties")
      projectProperties.load(fis)
      projectProperties
    },
    version := {
      val base = props.value.getProperty("project.version")
      val isRelease =
        sys.env
          .get("BUILD_RELEASE")
          .filter(_.nonEmpty)
          .orElse(Option(System.getProperty("buildRelease")))
          .exists(it => it == "true" || it == "1" || it == "yes" || it == "on")
      if (isRelease) base else s"$base-SNAPSHOT"
    }
  )
)

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val root = project
  .in(file("."))
  .settings(
    publish := {},
    publishLocal := {},
    // Task for triggering the Gradle build and publishing the artefacts
    // locally, because we depend on them
    publishLocalGradleDependencies := {
      import scala.sys.process.*
      val rootDir = (ThisBuild / baseDirectory).value
      val command = Process(
        "./gradlew" :: "publishToMavenLocal" :: Nil,
        rootDir
      )
      val log = streams.value.log
      val exitCode = command ! log
      if (exitCode != 0) {
        sys.error(s"Command failed with exit code $exitCode")
      }
    }
  )
  .aggregate(delayedqueueJVM)

lazy val delayedqueueJVM = project
  .in(file("delayedqueue-scala"))
  .settings(
    name := "delayedqueue-scala",
    libraryDependencies ++= Seq(
      "org.funfix" % "delayedqueue-jvm" % version.value,
      "org.typelevel" %% "cats-effect" % "3.6.3",
      // Testing
      "org.scalameta" %% "munit" % "1.0.4" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test,
      "org.typelevel" %% "cats-effect-testkit" % "3.6.3" % Test,
      "org.scalacheck" %% "scalacheck" % "1.19.0" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.2.0" % Test,
      // JDBC drivers for testing
      "com.h2database" % "h2" % "2.4.240" % Test,
      "org.hsqldb" % "hsqldb" % "2.7.4" % Test,
      "org.xerial" % "sqlite-jdbc" % "3.51.1.0" % Test,
    )
  )
