package root.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by faiaz on 16.10.16.
  */
trait SparkConfig {

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")

  implicit val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Ml")
    .getOrCreate()
}
