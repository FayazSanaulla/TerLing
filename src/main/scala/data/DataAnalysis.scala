package data

import config.SparkConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
/**
  * Created by faiaz on 14.10.16.
  */

object DataAnalysis extends SparkConfig {

  val schema: StructType =
    StructType(Seq(StructField("word", StringType), StructField("count", IntegerType)))

  def createDF(from: String, schema: StructType)
              (implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val rdd = rddFromFile(from)
      .map(clear)
      .flatMap(split)
      .filter(lengthPredicate)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(pair => Row(pair._1, pair._2))
    sqlContext.createDataFrame(rdd, schema)
  }

  private def lengthPredicate(str: String): Boolean = str.length > 2

  private def clear(str: String): String = str.replaceAll("[,.!?:]", "")

  private def split(str: String): Array[String] = str.split(" ")

  private def rddFromFile(path: String)(implicit sc: SparkContext): RDD[String] = sc.textFile(path)

}

