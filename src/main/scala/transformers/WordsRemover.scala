package transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable

/**
  * Created by faiaz on 13.01.17.
  */
class WordsRemover(override val uid: String = Identifiable.randomUID("linguisticparser"))
  extends Transformer
    with CustomTransformer {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  private val words = loadResources("/stopWords/english.txt")

  override def transform(dataset: Dataset[_]): DataFrame = {

    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata

    val t  = udf { arr: mutable.WrappedArray[String] =>
      arr
        .map(_.split(" ").filterNot(w => words.contains(w.toLowerCase)))
        .map(_.mkString(" "))
    }

    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, nullable = false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}
