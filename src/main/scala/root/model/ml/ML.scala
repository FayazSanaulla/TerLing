package root.model.ml

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import root.model.parser.SparkParsing
import root.util.SparkConfig


/**
  * Created by faiaz on 16.10.16.
  */
object ML extends App {

  val spark = new SparkParsing

  val positive = spark.clear("/home/faiaz/scala.txt")
  val negative = spark.clear("/home/faiaz/kotlin.txt")

  val tf = new HashingTF(numFeatures = 10000)

  val posFeature = positive.map(number => tf.transform(number.split(" ")))
  val negFeature = negative.map(number => tf.transform(number.split(" ")))

  val posExample = posFeature.map(num => LabeledPoint(1, num))
  val negExample = negFeature.map(num => LabeledPoint(0, num))

  val trainData = posExample.union(negExample)
  trainData.cache()

  val model = new LogisticRegressionWithLBFGS().run(trainData)

  println(s"pos result: ${model.predict(tf.transform("scala programming language".split(" ")))}")
  println(s"neg result: ${model.predict(tf.transform("fsdfasfasfafafasdfafa".split(" ")))}")
}
