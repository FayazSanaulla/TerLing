package pipeline

import config.SparkConfig
import epic.preprocess.MLSentenceSegmenter
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import transformers.TextCleaner

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import config.Utils.en_text_file
  import sqlContext.implicits._

  val segmenter = MLSentenceSegmenter.bundled().get

  val training = sc.textFile(en_text_file).flatMap(segmenter).toDF("sentences")

  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  val stopWordsRemover = new StopWordsRemover()
    .setInputCol(textCleaner.getOutputCol)
    .setOutputCol("filtered")

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover))

  val model = pipeline.fit(training)
//  model.transform(training).show()
}
