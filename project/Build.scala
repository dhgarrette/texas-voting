import sbt._
import Keys._

object TexasVotingBuild extends Build {

  lazy val main = Project("texas-voting", file(".")) dependsOn(scalautil)

  lazy val scalautil = Project("scala-util", file("./scala-util"))

}
