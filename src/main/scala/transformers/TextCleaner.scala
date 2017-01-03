package transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by faiaz on 01.01.17.
  */
class TextCleaner(override val uid: String) extends Transformer with CustomTransformer {

  def this() = this(Identifiable.randomUID("textcleaner"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, false))
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      sentences: String =>
        sentences
          .replaceAll("[,!?:]", "")
          .replaceAll("""\[[0-9]+]""", "")
          .replace("-", " ")
    }

    dataset.select(t(col($(inputCol))).as($(outputCol)))
  }
  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}