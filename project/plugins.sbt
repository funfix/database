addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.7")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.6.1")
addSbtPlugin("com.eed3si9n" % "sbt-salad-days" % "0.2.0")

// https://github.com/typelevel/sbt-tpolecat/issues/291
libraryDependencies += "org.typelevel" %% "scalac-options" % "0.1.11"
