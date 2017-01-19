package transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import utils.ResourceLoader

import scala.collection.mutable

/**
  * Created by faiaz on 08.01.17.
  */
class DangerousWordsTransformer(override val uid: String = Identifiable.randomUID("dangerEstimator"))
  extends Transformer
    with MultipleTransformer {
  import DangerousWordsTransformer._

  private val out: Array[String] = getOutputCols

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    //Counting of words danger
    val w = udf {
      arr: mutable.WrappedArray[String] =>
        val dangerWords = arr.flatMap(_.split(' ')
          .map(_.split('/').head)
          .map(w => words.find(_._1 == w).map(_._2).getOrElse(0.0)))

        dangerWords.sum / dangerWords.size
    }

    //Counting of associative pairs danger
    val p = udf {
      arr: mutable.WrappedArray[String] =>
        if (arr.size < 2) 0.0
        else {
          val dangerPairs = arr
            .map(_.split('/'))
            .map(_.partition(_.contains("NN")))
            .map {
              case (nouns, verbs) =>
                val pairs = for {
                  n <- nouns.map(_.split('/').head)
                  v <- verbs.map(_.split('/').head)
                } yield n -> v
                val swapped = pairs.map(_.swap)

                pairs ++ swapped
            }
            .flatMap(_.map(w => pairs.find(_._1 == w).map(_._2).getOrElse(0.0)))

          dangerPairs.sum / dangerPairs.size
        }
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

object DangerousWordsTransformer
  extends DefaultParamsReadable[DangerousWordsTransformer]
    with ResourceLoader{

  val words: Array[(String, Double)] = loadResources("/dangerous/dangerousWords.txt").map(w => {
    val splitRes = w.split("/")
    (splitRes(0), splitRes(1).toDouble)
  })

  val pairs: Array[((String, String), Double)] = loadResources("/dangerous/dangerousPairs.txt").map(w => {
    val splitRes = w.split("/")
    ((splitRes(0), splitRes(1)), splitRes(2).toDouble)
  })
  override def load(path: String): DangerousWordsTransformer = super.load(path)
}
