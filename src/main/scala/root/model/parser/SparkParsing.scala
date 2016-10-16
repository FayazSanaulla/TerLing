package root.model.parser

import root.util.SparkConfig

/**
  * Created by faiaz on 14.10.16.
  */
class SparkParsing extends SparkConfig {

  def count(classpath: String) = {
    sc.textFile(classpath)
    .map(str => str.replaceAll("[,.!?:]", ""))
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def clear(classpath: String) = {
    sc.textFile(classpath)
      .map(str => str.replaceAll("[,.!?:]", ""))
  }
}

