organization := "org.ditank.kafka.storage"

name := "kafka-storage"

scalaVersion := "2.12.7"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  //kafka
  "org.apache.kafka" %% "kafka" % "2.0.1",
  "org.apache.avro" % "avro" % "1.8.2",
  "io.confluent" % "kafka-avro-serializer" % "5.0.1",
  "io.confluent" % "kafka-streams-avro-serde" % "5.0.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1",
  "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts( Artifact("javax.ws.rs-api", "jar", "jar")), //needed to resolve kafka streams lib

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.github.pureconfig" %% "pureconfig" % "0.9.1",

  //test
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.mockito" % "mockito-core" % "2.23.0" % "test"
)

sourceGenerators in Test += (avroScalaGenerateSpecific in Test).taskValue
sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue
parallelExecution in Test := false

organization := "com.github.francescoditrani"
homepage := Some(url("https://github.com/francescoditrani/kafka-storage"))
scmInfo := Some(ScmInfo(url("https://github.com/francescoditrani/kafka-storage"), "git@github.com:francescoditrani/kafka-storage.git"))
developers := List(
  Developer("francescoditrani",
    "Francesco Ditrani",
    "f.ditrani@gmail.com",
    url("https://github.com/francescoditrani")),
  Developer("ankushkhanna",
    "Ankush Khanna",
    "ankushkhanna22@gmail.com",
    url("https://github.com/AnkushKhanna"))
)

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)