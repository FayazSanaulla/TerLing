package pipeline

import config.SparkConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec}

/**
  * Created by faiaz on 26.12.16.
  */
object Word2VecModel extends App with SparkConfig {
  import sqlContext.implicits._

  val training = sc.textFile("/home/faiaz/text.txt")
    .map(text => text.split("."))
    .map(Tuple1.apply)
    .toDF("text")
  val test = sc.textFile("/home/faiaz/test.txt")
    .map(text => text.split("."))
    .map(Tuple1.apply)
    .toDF("text")

  val word2vecModel = new Word2Vec()
    .setInputCol("result")
    .setOutputCol("final")
    .setVectorSize(10)
    .setMinCount(0)

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol("text")
    .setOutputCol("result")

  val pipeline = new Pipeline()
    .setStages(Array(stopWordsRemover, word2vecModel))

  pipeline.fit(training).transform(test).toDF("text", "result", "final").drop("text", "result").show()
}
