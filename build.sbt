
name := "big_data_poly"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"


javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.typesafe.akka" %% "akka-http" % "10.1.15",
  "com.typesafe.akka" %% "akka-actor" % "2.5.32",
  "com.typesafe.akka" %% "akka-stream" % "2.5.32",
  "ch.qos.logback" % "logback-classic" % "1.3.11",
  "io.spray" %% "spray-json" % "1.3.6"
)