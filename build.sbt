ThisBuild / organization := "darkages"
ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"
//ThisBuild / javaOptions ++= Seq("-source", "14", "-target", "14")

//ThisBuild / assemblyMergeStrategy := (_ => MergeStrategy.last)


lazy val commonSettings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.2.0",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      "org.postgresql" % "postgresql" % "42.2.16",
//      "org.slf4j" % "slf4j-api" % versions.slf4j,
//      "org.slf4j" % "log4j-over-slf4j" % versions.slf4j
      "com.lihaoyi" %% "upickle" % "2.0.0",
//      "org.snakeyaml" % "snakeyaml-engine" % "2.2.1",
    ),

)


lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "darkages",
    libraryDependencies ++= Seq(
//      "org.apache.kafka" % "kafka-clients" % "3.2.0",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
//      "org.postgresql" % "postgresql" % "42.2.16",
//      "org.slf4j" % "slf4j-api" % versions.slf4j,
//      "org.slf4j" % "log4j-over-slf4j" % versions.slf4j
      "com.lihaoyi" %% "upickle" % "2.0.0",
//      "org.snakeyaml" % "snakeyaml-engine" % "2.2.1",

      "org.scalatest" %% "scalatest" % "3.2.13" % "test",
      "com.h2database" % "h2" % "2.1.214" % Test

    ),
    assembly / assemblyJarName := "darkages.jar",
  )


