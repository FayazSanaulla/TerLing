package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import org.apache.spark.ml.feature.StopWordsRemover

/**
  * Created by faiaz on 31.12.16.
  */
object CustomePipeline extends App with SparkConfig {
  import sqlContext.implicits._

  private def removeUseless(str: String): String = {
    str.replaceAll("[,!?:]", "")
      .replaceAll("""\[[0-9]+]""", "")
      .replace("-", " ")

  }
  val segmenter = MLSentenceSegmenter.bundled().get

  val text = sc.textFile("/home/faiaz/testData/scala.txt")
    .map(removeUseless)
    .flatMap(segmenter)
    .map(_.replaceAll("\\.", ""))
    .map(_.split(" "))
    .toDF("sentences")

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol("sentences")
    .setOutputCol("filtered")

  stopWordsRemover.transform(text).drop("sentences").show()

}
