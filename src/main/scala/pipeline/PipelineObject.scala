package pipeline

import data.DataAnalysis
import data.DataAnalysis._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row}
import utils.Data._

/**
  * Created by faiaz on 07.12.16.
  */
object PipelineObject extends App {

  val training: DataFrame = DataAnalysis.createDF(scalaFile, schema)
  val test = DataAnalysis.createDF(other, schema)

  /*val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")
  val hashingTF = new HashingTF()
    .setNumFeatures(1000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")*/
  val lr = new LogisticRegression()
    .setMaxIter(20)
    .setRegParam(0.01)
  val pipeline = new Pipeline()
    .setStages(Array(lr))

  val model = pipeline.fit(training)
  save(MODEL_SAVE_PATH, model)

  val sameModel = load("/home/faiaz/spark-fitted-model")

  model.transform(test)
    .select("word", "count", "probability", "prediction")
    .collect()
    .foreach { case Row(word: String, count: String, prob: Vector, prediction: Double) =>
      println(s"($word, $count) --> prob=$prob, prediction=$prediction")
    }

  private def save(path: String, model: PipelineModel) = model.write.overwrite().save(path)
  private def load(path: String): PipelineModel = PipelineModel.load(path)
}
