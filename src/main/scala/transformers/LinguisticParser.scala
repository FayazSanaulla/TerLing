package transformers

import epic.sequences.CRF
import epic.trees.AnnotatedLabel
import linguistics._
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

  private def sentencesPart(word: String): SentencesParts = {
    val arrWord = word.split("/")
    val mWord = arrWord(0)
    val symbol = arrWord(1)
    symbol match {
      case "NN" => Noun(mWord)
      case "VB" => Verb(mWord)
      case "VBZ" => Verb3rdSinglePresent(mWord)
      case "VBP" => VerbNon3rdSinglePresent(mWord)
      case "VBN" => PastParticiple(mWord)
      case "VBG" => Gerund(mWord)
      case "JJ" => Adjective(mWord)
      case "RB" => Adverb(mWord)
      case "NNS" => NounPlural(mWord)
      case "IN" => Conjunction(mWord)
      case "CD" => Date(mWord)
      case "NNP" => ProperNounSingle(mWord)
      case "NNPS" => ProperNounPlural(mWord)
      case other => Unknown(mWord, other)
    }
  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val t = udf {
      arr: mutable.WrappedArray[String] =>
        tg.bestSequence(arr)
          .render
          .split(" ")
          .map(sentencesPart)
    }
    dataset.select(t(col($(inputCol))).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}
