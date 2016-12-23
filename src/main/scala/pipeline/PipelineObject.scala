package pipeline

import config.SparkConfig
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by faiaz on 07.12.16.
  */
object PipelineObject extends App with SparkConfig {

  val training: DataFrame = spark.createDataFrame(Seq(
    (0L, "testing normal", 1.0),
    (1L, "smart", 0.0),
    (2L, "spark mllib", 1.0),
    (3L, "hadoop mapreduce", 0.0)
  )).toDF("id", "text", "label")

  val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")
  val hashingTF = new HashingTF()
    .setNumFeatures(1000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
  val lr = new LogisticRegression()
    .setMaxIter(20)
    .setRegParam(0.01)
  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, hashingTF, lr))

  val model = pipeline.fit(training)
  model.write.overwrite().save("/home/faiaz/spark-fitted-model")

  val sameModel = PipelineModel.load("/home/faiaz/spark-fitted-model")

  val test = spark.createDataFrame(Seq(
    (0L, "testing normal"),
    (1L, "smart"),
    (2L, "spark mllib"),
    (3L, "hadoop mapreduce")
  )).toDF("id", "text")

  model.transform(test)
    .select("id", "text", "probability", "prediction")
    .collect()
    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }
}
