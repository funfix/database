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
      "-no-indent"
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

lazy val delayedqueue = crossProject(JVMPlatform)
  .crossType(CrossType.Full)
  .in(file("delayedqueue-scala"))
  .settings(
    name := "delayedqueue-scala"
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.funfix" % "delayedqueue-jvm" % version.value,
      // Testing
      "org.scalameta" %% "munit" % "1.0.4" % Test
    )
  )

lazy val delayedqueueJVM = delayedqueue.jvm
