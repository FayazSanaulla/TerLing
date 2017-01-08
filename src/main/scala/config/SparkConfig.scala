package config

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by faiaz on 16.10.16.
  */
trait SparkConfig {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")
    .set("spark.cores.max", "4")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

}
