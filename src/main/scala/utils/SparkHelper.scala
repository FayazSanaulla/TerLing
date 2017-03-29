package utils

import config.SparkConfig
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
  * Created by faiaz on 13.01.17.
  */
trait SparkHelper extends SparkConfig {
  import spark.implicits._

  final def print(df: DataFrame): Unit = df.collect().foreach(println)

  final def loadSeqDF(name: String, label: Double): DataFrame = {
    sc.wholeTextFiles(s"file:///home/faiaz/IdeaProjects/spark/src/main/resources/data$name")
      .map(_._2)
      .toDF("sentences")
      .withColumn("label", lit(label))
  }

  final def loadTrainDF(path: String): DataFrame = {
    sc.textFile(s"file:///home/faiaz/IdeaProjects/spark/src/main/resources/data$path").toDF("sentences")
  }

  final def loadData(path: String): DataFrame = sc.textFile(path).toDF("sentences")

  final def saveModel(model: PipelineModel, path: String): Unit = {
    model.write.overwrite().save(path)
  }

  final def loadModel(path: String): PipelineModel = {
    PipelineModel.load(path)
  }
}
