package transformers

import epic.preprocess.MLSentenceSegmenter
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by faiaz on 01.01.17.
  */
class TextCleaner(override val uid: String = Identifiable.randomUID("textCleaner"))
  extends Transformer
    with SingleTransformer {

  private implicit val segmenter: MLSentenceSegmenter = MLSentenceSegmenter.bundled().get

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, nullable = false))
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      sentences: String =>
        sentences
          .trim
          .flatMap(segmenter)
          .map(_.replaceAll("[,!?:\\.&^%$*@()]", "")
                .replaceAll("""\[[0-9]+]""", "")
                .replace('-', ' ')
                .split(' ')
                .filterNot(_ == "")
                .distinct
                .mkString(" ")
          )
    }
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol)))
  }
  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}

object TextCleaner extends DefaultParamsReadable[TextCleaner] {
  override def load(path: String): TextCleaner = super.load(path)
}