package root.model.parser

import org.apache.spark.SparkContext
import root.util.SparkConfig

/**
  * Created by faiaz on 14.10.16.
  */
object SparkParsing extends SparkConfig {

  def count(from: String, to: String)(implicit sc: SparkContext) = {
    rddFromFile(from)
      .map(clear)
      .flatMap(split)
      .filter(lengthPredicate)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(to)
  }

  def lengthPredicate(str: String): Boolean = str.length > 2

  def clear(str: String): String = str.replaceAll("[,.!?:]", "")

  def split(str: String): Array[String] = str.split(" ")

  def rddFromFile(path: String)(implicit sc: SparkContext) = sc.textFile(path)

}

