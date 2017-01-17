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

  val loadPath: String

  final def print(df: DataFrame): Unit = df.collect().foreach(println)

  final def loadSeqDF(name: String, label: Double): DataFrame = {
    sc.wholeTextFiles(s"file:///home/faiaz/IdeaProjects/spark/src/main/resources/data$name")
      .map(_._2)
      .toDF("sentences")
      .withColumn("label", lit(label))
  }

  final def loadDF(path: String): DataFrame = {
    sc.textFile(s"file:///home/faiaz/IdeaProjects/spark/src/main/resources/data$path")
      .toDF("sentences")
  }

  final def saveModel(model: PipelineModel): Unit = {
    model.write.overwrite().save(loadPath)
  }

  final def loadModel(path: String): PipelineModel = {
    PipelineModel.load(path)
  }
}
