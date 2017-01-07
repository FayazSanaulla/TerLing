package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import transformers.{LinguisticParser, TextCleaner}

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import config.Utils.en_text_file
  import sqlContext.implicits._

  implicit val tagger: CRF[AnnotatedLabel, String] = epic.models.PosTagSelector.loadTagger("en").get

  val segmenter = MLSentenceSegmenter.bundled().get

  val training = sc.textFile(en_text_file).flatMap(segmenter).toDF("sentences")

  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol(textCleaner.getOutputCol)
    .setOutputCol("filtered")

  val testFilter = new LinguisticParser()
    .setInputCol(stopWordsRemover.getOutputCol)
    .setOutputCol("filtered1")

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover, testFilter))

//  val model = pipeline.fit(training)
//  model.transform(training).show()

  val tc = textCleaner.transform(training)
  val swr = stopWordsRemover.transform(tc)
  val tf = testFilter.transform(swr)
  //swr.drop("cleaned").collect().foreach(println)
  tf.collect().foreach(println)
}
