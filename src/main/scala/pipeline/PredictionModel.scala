package pipeline

import utils.SparkHelper

/**
  * Created by faiaz on 29.03.17.
  */
object PredictionModel extends App with SparkHelper {

  val path: String = "/home/faiaz/model"

  val positiveTest = loadTrainDF("/test/positive.txt")
  val negativeTest = loadTrainDF("/test/negative.txt")
  val randomTest = loadData("random.txt")

  val model = loadModel(path)

  //PREDICTION
  val prediction = model.transform(randomTest)
    .select("sentences", "probability", "prediction")

  print(prediction)
}
