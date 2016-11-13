package root.model.ml

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import root.model.parser.SparkParsing._


/**
  * Created by faiaz on 16.10.16.
  */
object ML extends App {

  val positive = rddFromFile("/home/faiaz/scala.txt").map(clear)
  val negative = rddFromFile("/home/faiaz/other.txt").map(clear)

  val tf = new HashingTF(numFeatures = 10000)

  val posFeature = positive.map(number => tf.transform(number.split(" ")))
  val negFeature = negative.map(number => tf.transform(number.split(" ")))

  val posExample = posFeature.map(num => LabeledPoint(1, num))
  val negExample = negFeature.map(num => LabeledPoint(0, num))

  val trainData = posExample.union(negExample)
  trainData.cache()

  val model = new LogisticRegressionWithLBFGS().run(trainData)

  println(s"pos result: ${model.predict(tf.transform("adsfs adfs adfsadfsad asdaa".split(" ")))}")
  println(s"neg result: ${model.predict(tf.transform("sdfa dfas fasdfsadfs dfsadf".split(" ")))}")
}
