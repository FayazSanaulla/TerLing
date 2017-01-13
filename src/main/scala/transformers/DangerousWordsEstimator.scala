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

  private val words = loadResources("/dangerous/dangerousWords.txt").map(w => {
    val splitRes = w.split("/")
    (splitRes(0), splitRes(1).toDouble)
  }).toMap

  private val associationPairs = loadResources("/dangerous/dangerousPairs.txt").map(w => {
    val splitRes = w.split("/")
    ((splitRes(0), splitRes(1)), splitRes(2).toDouble)
  }).toMap

  def setInputCol(value: String): this.type = set(inputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val wordsDanger = udf {
      arr: mutable.WrappedArray[String] =>
        val res = arr.map(_.split("/")).map(w => words.getOrElse(w.head, 0.0))

        res.sum / res.size
    }

    val pairsDanger = udf {
      arr: mutable.WrappedArray[String] =>
        val (nouns, verbs) = arr.partition(pair => pair.contains("NN"))
        val pairs = for {
          n <- nouns.map(_.split("/").head)
          v <- verbs.map(_.split("/").head)
        } yield n -> v

        val swapped = pairs.map(_.swap)

        val resPairs = pairs ++ swapped

        val size = resPairs.size
        val sum = resPairs.map(w => associationPairs.getOrElse(w, 0.0)).sum

        4 * sum / size
    }

//    dataset
//      .select(
//        split(concat_ws("/", avg(wordsDanger(col($(inputCol)))), avg(pairsDanger(col($(inputCol))))), "/").as("features"),
//        col("*")
//      )
//    dataset
//      .select(
//        avg(wordsDanger(col($(inputCol)))).as("words"),
//        avg(pairsDanger(col($(inputCol)))).as("pairs")
//      )
    dataset
      .select(
        col("*"),
        wordsDanger(col($(inputCol))),
        pairsDanger(col($(inputCol)))
      )
      .drop("parsed")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol.name, StringType, nullable = false))
  }

  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}
