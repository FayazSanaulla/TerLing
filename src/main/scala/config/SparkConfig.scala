package config

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by faiaz on 16.10.16.
  */
trait SparkConfig {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TerLing")
    .set("spark.executor.memory", "8g")
    .set("spark.driver.memory", "16g")
    .set("spark.cores.max","4g")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
}
