package pipeline

import config.SparkConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions.lit
import transformers.{DangerousWordsEstimator, LinguisticParser, TextCleaner, WordsRemover}

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import sqlContext.implicits._
  import utils.Helper._

  val training = sc.textFile("file:///home/faiaz/IdeaProjects/big_data/src/main/resources/data/en_text_1.txt")
    .toDF("sentences")
    .withColumn("label", lit(1.0))
    .cache()

  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  val stopWordsRemover = new WordsRemover()
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

//  val model = pipeline.fit(training)

  val tc = textCleaner.transform(training)
  val swr = stopWordsRemover.transform(tc)
  val lp = lingParser.transform(swr)
  print(lp)
//  lp.show()
//  val est = dangerousEstimator.transform(lp)
//  est.show()
}
