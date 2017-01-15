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
  val training = loadDF("en_text", label = true).cache()
  val test = loadDF("en_text_1").cache()
  val terror = loadDF("terror").cache()

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
    .setStages(Array(textCleaner, stopWordsRemover, lingParser, dangerousEstimator, vectorAssembler, logReg))

  //MODEL
  val model = pipeline.fit(training)

  //PREDICTION
  model.transform(terror)
    .select("sentences", "probability", "prediction")
    .show()
}
