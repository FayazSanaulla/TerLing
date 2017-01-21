package transformers

import net.didion.jwnl.data.POS
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import utils.ResourceLoader
import wordnet.WordNetService

import scala.collection.immutable.Seq
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
        val (nouns, verbs) = arr.flatMap(_.split(' ')).partition(_.contains("NN"))
        val nounsEst = nouns.map(_.split('/').head)
          .map(w => nounsSets.find(_._1.contains(w)).map(_._2).getOrElse(0.0))
        val verbsEst = verbs.map(_.split('/').head)
          .map(w => verbsSets.find(_._1.contains(w)).map(_._2).getOrElse(0.0))
        val resArr = nounsEst ++ verbsEst

        resArr.sum / resArr.size

    }

    //Counting of associative pairs danger
    val p = udf {
      arr: mutable.WrappedArray[String] =>
        if (arr.size < 2) 0.0
        else {
//          val dangerPairs = arr.map(_.split('/'))
//            .map(_.partition(_.contains("NN")))
//            .map {
//              case (n, v) =>
//                val pairs = for {
//                  n <- n.map(_.split('/').head)
//                  v <- v.map(_.split('/').head)
//                } yield n -> v
//                val swapped = pairs.map(_.swap)
//
//                pairs ++ swapped
//            }
//            .flatMap(_.map(w => pairs.find(_._1 == w).map(_._2).getOrElse(0.0)))
//
//          dangerPairs.sum / dangerPairs.size
          val dangerPairs = arr.map(_.split('/'))
            .map(_.partition(_.contains("NN")))
            .map { case(n, v) =>
                val nouns = n.map(_.split('/').head)
                val verbs = v.map(_.split('/').head)
                val pairs = for {
                  n <- nouns
                  v <- verbs
                } yield n -> v
                val swappedPairs = pairs.map(_.swap)
                pairs ++ swappedPairs
          }
            .map(p => pairsSets.find(_._1 == p).map(_._2).getOrElse(0.0))

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
    with ResourceLoader {

  val wns = new WordNetService

  val nounsSets: Array[(Seq[String], Double)] = loadResources("/dangerous/dangerousNouns.txt").map(w => {
    val splitRes = w.split("/")
    (splitRes(0), splitRes(1).toDouble)
  }).map { case(word, estimate) => wns.synonyms(word, POS.NOUN) -> estimate }

  val verbsSets: Array[(Seq[String], Double)] = loadResources("/dangerous/dangerousVerbs.txt")
    .map(w => {
    val splitRes = w.split('/')
    (splitRes(0), splitRes(1).toDouble)
  }).map { case(word, estimator) => wns.synonyms(word, POS.VERB) -> estimator}

  val pairsSets: Array[((String, String), Double)] = loadResources("/dangerous/dangerousPairs.txt").map(w => {
    val splitRes = w.split("/")
    ((splitRes(0), splitRes(1)), splitRes(2).toDouble)
  })

  override def load(path: String): DangerousWordsTransformer = super.load(path)
}
