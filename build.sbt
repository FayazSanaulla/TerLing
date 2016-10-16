name := "big_data"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.1"
)
