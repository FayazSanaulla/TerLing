package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import transformers.{DangerousWordsEstimator, LinguisticParser, TextCleaner}

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import config.Paths._
  import sqlContext.implicits._

  implicit val tagger: CRF[AnnotatedLabel, String] = epic.models.PosTagSelector.loadTagger("en").get

  val segmenter = MLSentenceSegmenter.bundled().get

  val training = sc.textFile(en_text_file_1).flatMap(segmenter).toDF("sentences").cache()
  val sample = sc.textFile(en_text_file).flatMap(segmenter).toDF("sentences").cache()

  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol(textCleaner.getOutputCol)
    .setOutputCol("filtered")

  val lingParser = new LinguisticParser()
    .setInputCol(stopWordsRemover.getOutputCol)
    .setOutputCol("parsed")

  val dangerousEstimator = new DangerousWordsEstimator()
    .setInputCol(lingParser.getOutputCol)
    .setOutputCol("estimated")

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover, lingParser, dangerousEstimator))

  val tc = textCleaner.transform(training)
  val swr = stopWordsRemover.transform(tc)
  val tf = lingParser.transform(swr)
  val est = dangerousEstimator.transform(tf)
  est.collect().foreach(println)
}
