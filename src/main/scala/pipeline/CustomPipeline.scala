package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import org.apache.spark.ml.feature.StopWordsRemover
import pipeline.transformers.TextCleaner

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import sqlContext.implicits._

  private def removeUselessSymbols(str: String): String = {
    str.replaceAll("[,!?:]", "")
      .replaceAll("""\[[0-9]+]""", "")
      .replace("-", " ")

  }
  val segmenter = MLSentenceSegmenter.bundled().get

  val text = sc.textFile("/home/faiaz/testData/scala.txt")
    .map(removeUselessSymbols)
    .flatMap(segmenter)
    .map(_.replaceAll("\\.", ""))
    .map(_.split(" "))
    .toDF("sentences")

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol("sentences")
    .setOutputCol("filtered")

  val test = sc.textFile("/home/faiaz/testData/scala.txt").flatMap(segmenter).toDF("sentences")

  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  //stopWordsRemover.transform(text).drop("sentences").show()
  //textCleaner.transform(test).show()
  test.show()

}
