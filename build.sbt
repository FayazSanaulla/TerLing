name := "big_data"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.scalanlp" % "epic_2.11" % "0.4-4f38c5718c8fb78d928b57c9fae3649fb0e2f08f",
  "org.scalanlp" % "epic-pos-en_2.11" % "2016.8.29",
  "org.scalanlp" % "epic-parser-en-span_2.11" % "2016.8.29"
)
