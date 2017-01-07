package transformers

import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
/**
  * Created by faiaz on 07.01.17.
  */
class LinguisticParser(override val uid: String = Identifiable.randomUID("linguisticparser"))
                      (implicit tg: CRF[AnnotatedLabel, String])
  extends Transformer with CustomTransformer {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      arr: mutable.WrappedArray[String] => tg.bestSequence(arr).render
    }
    dataset.select(t(col($(inputCol))).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}
