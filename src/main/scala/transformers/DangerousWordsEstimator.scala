package transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import utils.ResourceLoader

import scala.collection.mutable

/**
  * Created by faiaz on 08.01.17.
  */
class DangerousWordsEstimator(override val uid: String = Identifiable.randomUID("dangerEstimator"))
  extends Transformer
    with MultipleTransformer
    with ResourceLoader {

  private val words = loadResources("/dangerous/dangerousWords.txt").map(w => {
    val splitRes = w.split("/")
    (splitRes(0), splitRes(1).toDouble)
  }).toMap

  private val pairs = loadResources("/dangerous/dangerousPairs.txt").map(w => {
    val splitRes = w.split("/")
    ((splitRes(0), splitRes(1)), splitRes(2).toDouble)
  }).toMap

  private var out: Array[String] = Array.empty[String]

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: Array[String]): this.type = {
    out = value
    set(outputCol, value)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val w = udf {
      arr: mutable.WrappedArray[String] =>

        //Counting of words danger
        val dangerWords = arr.flatMap(_.split(" ")
          .map(_.split("/").head)
          .map(w => words.getOrElse(w, 0.0)))

        val wordRes = dangerWords.sum / dangerWords.size
        wordRes
    }

    val p = udf {
      arr: mutable.WrappedArray[String] =>

        //Counting of assotiative pairs danger
        val dangerPairs = arr
          .map(_.split(" "))
          .map(_.partition(_.contains("NN")))
          .map { case (nouns, verbs) => {
              val pairs = for {
                n <- nouns.map(_.split("/").head)
                v <- verbs.map(_.split("/").head)
              } yield n -> v
              val swapped = pairs.map(_.swap)

              pairs ++ swapped
            }
          }
          .flatMap(_.map(w => pairs.getOrElse(w, 0.0)))

        val pairsRes = dangerPairs.sum / dangerPairs.size
        pairsRes
    }

    dataset.select(
      col("*"),
      w(col($(inputCol))).as(out.head),
      p(col($(inputCol))).as(out.last)
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields :+
        StructField(out.head, DoubleType, nullable = false) :+
        StructField(out.last, DoubleType, nullable = false)
    )
  }
  override def copy(extra: ParamMap): TextCleaner = {defaultCopy(extra)}
}
