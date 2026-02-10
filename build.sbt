import java.io.FileInputStream
import java.util.Properties
import sbt.ThisBuild
import scala.sys.process.Process

val scala3Version = "3.3.7"
val scala2Version = "2.13.18"

val publishLocalGradleDependencies =
  taskKey[Unit]("Builds and publishes gradle dependencies")

val props =
  settingKey[Properties]("Main project properties")

inThisBuild(
  Seq(
    organization := "org.funfix",
    scalaVersion := scala2Version,
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

val sharedSettings = Seq(
  crossScalaVersions := Seq(scala3Version, scala2Version),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) =>
        Seq("-no-indent")
      //          Seq.empty
      case Some((2, _)) | _ =>
        Seq("-Xsource:3-cross")
    }
  },
  Compile / compile / wartremoverErrors ++= Seq(
    Wart.Null,
    Wart.AsInstanceOf,
    Wart.ExplicitImplicitTypes,
    Wart.FinalCaseClass,
    Wart.FinalVal,
    Wart.ImplicitConversion,
    Wart.IsInstanceOf,
    Wart.JavaSerializable,
    Wart.LeakingSealed,
    Wart.NonUnitStatements,
    Wart.TripleQuestionMark,
    Wart.TryPartial,
    Wart.Return,
    Wart.PublicInference,
    Wart.OptionPartial,
    Wart.ArrayEquals
  ),
  organization := "org.funfix",
  organizationName := "Funfix",
  organizationHomepage := Some(url("https://funfix.org")),

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/funfix/database"),
      "scm:git@github.com:funfix/database.git"
    )
  ),
  developers := List(
    Developer(
      id = "alexelcu",
      name = "Alexandru Nedelcu",
      email = "noreply@alexn.org",
      url = url("https://alexn.org")
    )
  ),

  description := "DelayedQueue implementation for Scala.",
  licenses := List(License.Apache2),
  homepage := Some(url("https://github.com/funfix/database")),

  // Remove all additional repository other than Maven Central from POM
  pomIncludeRepository := { _ => false },
  publishMavenStyle := true,

  // new setting for the Central Portal
  publishTo := {
    val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
    if (version.value.endsWith("-SNAPSHOT")) Some("central-snapshots".at(centralSnapshots))
    else localStaging.value
  },

  // ScalaDoc settings
  autoAPIMappings := true,
  scalacOptions ++= Seq(
    // Note, this is used by the doc-source-url feature to determine the
    // relative path of a given source file. If it's not a prefix of a the
    // absolute path of the source file, the absolute path of that file
    // will be put into the FILE_SOURCE variable, which is
    // definitely not what we want.
    "-sourcepath",
    file(".").getAbsolutePath.replaceAll("[.]$", ""),
    // Debug warnings
    "-Wconf:any:warning-verbose"
  )
)

lazy val root = project
  .in(file("."))
  .settings(
    publish := {},
    publishLocal := {},
    crossScalaVersions := Nil,
    // Task for triggering the Gradle build and publishing the artefacts
    // locally, because we depend on them
    publishLocalGradleDependencies := {
      import scala.sys.process.*
      val rootDir = (ThisBuild / baseDirectory).value
      val commandArgs =
        List(
          List("./gradlew"),
          if (!version.value.endsWith("-SNAPSHOT")) List("-PbuildRelease=true") else Nil,
          List("publishToMavenLocal")
        ).flatten

      val command = Process(commandArgs, rootDir)
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
  .settings(sharedSettings)
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
      "org.xerial" % "sqlite-jdbc" % "3.51.1.0" % Test
    )
  )

addCommandAlias(
  "ci-test",
  ";publishLocalGradleDependencies;+test;scalafmtCheckAll"
)
addCommandAlias(
  "ci-publish-local",
  ";publishLocalGradleDependencies; +Test/compile; +publishLocal"
)
addCommandAlias(
  "ci-publish",
  ";publishLocalGradleDependencies; +Test/compile; +publishSigned; sonaUpload"
)
