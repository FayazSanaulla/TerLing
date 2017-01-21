package pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import transformers.{DangerousWordsTransformer, LinguisticParser, TextCleaner, WordsRemover}
import utils.SparkHelper

/**
  * Created by faiaz on 31.12.16.
  */
object CustomPipeline extends App with SparkHelper {

  override val loadPath: String = "/tmp/fitted-model-log-reg"

  //DATA
  val positive = loadSeqDF("/pos", 1.0)
  val negative = loadSeqDF("/neg", 0.0)
  val training = positive.union(negative)

  val test = loadDF("/test/test.txt")

  //STAGES
  val textCleaner = new TextCleaner()
    .setInputCol("sentences")
    .setOutputCol("cleaned")

  val wordsRemover = new WordsRemover()
    .setInputCol(textCleaner.getOutputCol)
    .setOutputCol("filtered")

  val lingParser = new LinguisticParser()
    .setInputCol(wordsRemover.getOutputCol)
    .setOutputCol("parsed")

  val dangerousEstimator = new DangerousWordsTransformer()
    .setInputCol(lingParser.getOutputCol)
    .setOutputCols(Array("word", "pair"))

  val vectorAssembler = new VectorAssembler()
    .setInputCols(dangerousEstimator.getOutputCols)
    .setOutputCol("features")

  val logReg = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol(vectorAssembler.getOutputCol)
    .setMaxIter(10)
    .setRegParam(0.001)

  val pipeline = new Pipeline()
    .setStages(Array(textCleaner, wordsRemover, lingParser, dangerousEstimator, vectorAssembler, logReg))

//  //MODEL
//  val model = pipeline.fit(training)
//
//  //PREDICTION
//  val prediction = model.transform(test)
//    .select("sentences", "features", "probability", "prediction")
//
//  print(prediction)
  val tc = textCleaner.transform(test)
  val wr = wordsRemover.transform(tc)
  val lp = lingParser.transform(wr)
  val da = dangerousEstimator.transform(lp)
  da.show()
}
