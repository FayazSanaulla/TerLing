package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import transformers.{DangerousWordsEstimator, LinguisticParser, TextCleaner}

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import sqlContext.implicits._

  def print(df: DataFrame): Unit = df.collect().foreach(println)

  implicit val tagger: CRF[AnnotatedLabel, String] = epic.models.PosTagSelector.loadTagger("en").get

  val segmenter = MLSentenceSegmenter.bundled().get

  val training = sc.textFile("file:///home/faiaz/IdeaProjects/spark/src/main/resources/data/en_text_1.txt")
    .flatMap(segmenter)
    .toDF("sentences")
    .cache()

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

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover, lingParser, dangerousEstimator, lr))

  val tc = textCleaner.transform(training)
  val swr = stopWordsRemover.transform(tc)
  val tf = lingParser.transform(swr)
  val est = dangerousEstimator.transform(tf)
  est.show()
}
