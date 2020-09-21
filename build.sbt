name := "Interview"

version := "0.1"

//scalaVersion := "2.12.7"
scalaVersion := "2.12.8"

scalacOptions += "-Ypartial-unification"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"


val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.12.3")


resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v0.35.0"


val sparkVersion = "2.4.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.mockito" % "mockito-core" % "2.8.47" % "test"
)


libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

