package root.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by faiaz on 16.10.16.
  */
trait SparkConfig {

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")

  val sc = new SparkContext(conf)
}
