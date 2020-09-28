name := "Interview"

version := "0.1"

scalaVersion := "2.12.8"
scalacOptions += "-Ypartial-unification"

val sparkVersion = "2.4.2"
val circeVersion = "0.12.3"
name := "parse-csv"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.mrpowers" % "spark-daria" % "v0.35.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "com.github.melrief" %% "purecsv" % "0.1.1"
)