addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.0")
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.3")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.5.7")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

// https://github.com/typelevel/sbt-tpolecat/issues/291
libraryDependencies += "org.typelevel" %% "scalac-options" % "0.1.10"
