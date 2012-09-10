import sbt._
import Keys._

object TexasVotingBuild extends Build {

  lazy val main = Project("texas-voting", file(".")) dependsOn(scalabha)

  lazy val scalabha = Project("Scalabha", file("./scalabha"))

}

