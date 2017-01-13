package pipeline

import config.SparkConfig
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

  val training = sc.textFile("file:///home/faiaz/IdeaProjects/big_data/src/main/resources/data/en_text_1.txt")
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
    .setRegParam(0.001)

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover, lingParser, dangerousEstimator, lr))

  val tc = textCleaner.transform(training)
  val swr = stopWordsRemover.transform(tc)
  val tf = lingParser.transform(swr)
  val est = dangerousEstimator.transform(tf)
  est.show()
}
