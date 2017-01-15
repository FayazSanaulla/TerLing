package utils

import config.SparkConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
  * Created by faiaz on 13.01.17.
  */
trait SparkHelper extends SparkConfig {
  import sqlContext.implicits._

  final def print(df: DataFrame): Unit = df.collect().foreach(println)

  final def loadDF(name: String, label: Boolean = false): DataFrame = {
    val df = sc.wholeTextFiles(s"file:///home/faiaz/IdeaProjects/spark/src/main/resources/data/$name.txt")
      .map(_._2)
      .toDF("sentences")
    if (label) df.withColumn("label", lit(1.0)) else df
  }
}
