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
  import DangerousWordsEstimator._

  private val words = loadDangerousWords.map(w => {
    val splitRes = w.split("/")
    (splitRes(0), splitRes(1).toDouble)
  }).toMap

  private val associationPairs = loadDangerousPairs.map(w => {
    val splitRes = w.split("/")
    ((splitRes(0), splitRes(1)), splitRes(2).toDouble)
  }).toMap

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)
  //todo: fix spark class cast error
  override def transform(dataset: Dataset[_]): DataFrame = {

    val wordCount = udf {
      arr: mutable.WrappedArray[(String, String)] =>
        val nArr = arr.map(_._1)

        val size = nArr.size
        val sum = nArr.map(w => words.getOrElse(w, 0.0)).sum

        sum / size
    }

    val associationPair = udf {
      arr: mutable.WrappedArray[(String, String)] =>
        val (nouns, verbs) = arr.partition(pair => pair._2.contains("NN"))
        val pairs = for {
          n <- nouns.map(_._1)
          v <- verbs.map(_._1)
        } yield n -> v

        val size = pairs.size
        val sum = pairs.map(w => associationPairs.getOrElse(w, 0.0)).sum

        sum / size
    }

    dataset
      .select(
        avg(wordCount(col($(inputCol)))).as("words"),
        avg(associationPair(col($(inputCol)))).as("pairs"))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, nullable = false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}

object DangerousWordsEstimator {

  def loadDangerousWords: Array[String] = {
    val is = getClass.getResourceAsStream("/dangerous/DangerousWords.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }

  def loadDangerousPairs: Array[String] = {
    val is = getClass.getResourceAsStream("/dangerous/DangerousPairs.txt")
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
