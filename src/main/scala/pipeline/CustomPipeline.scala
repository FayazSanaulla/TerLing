package pipeline

import config.SparkConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.lit
import transformers.{DangerousWordsEstimator, LinguisticParser, TextCleaner, WordsRemover}

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkConfig {
  import sqlContext.implicits._

  //DATA
  val training = sc.wholeTextFiles("file:///home/faiaz/IdeaProjects/spark/src/main/resources/data/en_text_1.txt")
    .map(_._2)
    .toDF("sentences")
    .withColumn("label", lit(1.0))
    .cache()

  val test = sc.wholeTextFiles("file:///home/faiaz/IdeaProjects/spark/src/main/resources/data/en_text.txt")
    .map(_._2)
    .toDF("sentences")
    .cache()

  //STAGES
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
    .setOutputCol(Array("word", "pair"))

  val vectorAssembler = new VectorAssembler()
    .setInputCols(dangerousEstimator.getOutputCol)
    .setOutputCol("features")

  val logReg = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol(vectorAssembler.getOutputCol)
    .setMaxIter(10)
    .setRegParam(0.001)

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, stopWordsRemover, lingParser, dangerousEstimator, vectorAssembler, logReg))

  //MODEL
//  val model = pipeline.fit(training)
//
//  model.transform(test)
//    .select("sentences", "probability", "prediction")
//    .show()
  test.show()
}
