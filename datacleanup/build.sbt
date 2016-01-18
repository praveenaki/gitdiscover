name := "dataCleanUp"

organization := "com.virdis"

version := "0.0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test" withSources() withJavadoc(),
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test" withSources() withJavadoc(),
  "com.typesafe.play" % "play-json_2.10" % "2.4.6"
)

initialCommands := "import play.api.libs.json._ \n" +
  "import com.virdis.datacleanup._"

