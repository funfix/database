addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.6")
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.2")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.5.5")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")

// https://github.com/typelevel/sbt-tpolecat/issues/291
libraryDependencies += "org.typelevel" %% "scalac-options" % "0.1.9"
