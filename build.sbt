name := "texas-voting"

version := "0.0.1"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")


libraryDependencies ++= Seq(
  "net.sf.opencsv" % "opencsv" % "2.0",
  "net.sourceforge.htmlunit" % "htmlunit" % "2.10",
  "org.apache.pdfbox" % "pdfbox" % "1.7.1")

mainClass in (Compile, run) := None

