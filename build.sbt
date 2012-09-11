name := "texas-voting"

version := "0.0.1"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")


libraryDependencies ++= Seq(
  "net.sourceforge.htmlunit" % "htmlunit" % "2.10")

mainClass in (Compile, run) := None

