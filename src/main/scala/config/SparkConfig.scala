package config

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by faiaz on 16.10.16.
  */
trait SparkConfig {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")

  implicit val sc = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Ml")
    .getOrCreate()
}
