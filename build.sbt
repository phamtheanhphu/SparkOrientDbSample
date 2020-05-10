name := "SparkOrientDbSample"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "com.orientechnologies" % "orientdb-graphdb" % "3.1.0-M3",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)