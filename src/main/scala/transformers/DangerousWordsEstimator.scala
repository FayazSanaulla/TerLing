package transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable

/**
  * Created by faiaz on 08.01.17.
  */
class DangerousWordsEstimator(override val uid: String = Identifiable.randomUID("dangerEstimator"))
  extends Transformer
    with CustomTransformer {
  import DangerousWordsEstimator.loadDangerousWords

  private val words = loadDangerousWords.map(w => {
    val arr = w.split("/")
    (arr(0), arr(1).toDouble)
  }).toMap

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      arr: mutable.WrappedArray[String] =>
        arr.map(w => (w, words.getOrElse(w, 0.0)))
    }
    dataset.select(t(col($(inputCol))).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, nullable = false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}

object DangerousWordsEstimator {
  def loadDangerousWords: Array[String] = {
    val is = getClass.getResourceAsStream("/dangerous/nouns.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
