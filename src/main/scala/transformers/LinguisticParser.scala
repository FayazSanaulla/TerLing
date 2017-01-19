package transformers

import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
/**
  * Created by faiaz on 07.01.17.
  */
class LinguisticParser(override val uid: String = Identifiable.randomUID("linguisticParser"))
  extends Transformer
    with SingleTransformer {
  import LinguisticParser._

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      arr: mutable.WrappedArray[String] =>
        arr.map(str =>
          tagger.bestSequence(str.split(' '))
          .render.split(' ')
          .filter(x => x.contains("NN") || x.contains("VB"))
          .mkString(" ")
        )
    }
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}

object LinguisticParser extends DefaultParamsReadable[LinguisticParser] {

  val tagger: CRF[AnnotatedLabel, String] = epic.models.PosTagSelector.loadTagger("en").get

  override def load(path: String): LinguisticParser = super.load(path)
}

