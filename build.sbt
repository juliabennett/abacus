name := "streamProcessing"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.8",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.8" % Test
)
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.8",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.8" % Test
)
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.11" % Test,
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.0-RC1"
)
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"