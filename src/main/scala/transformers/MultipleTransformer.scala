package transformers

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by faiaz on 15.01.17.
  */
trait MultipleTransformer extends Params {

  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)

  final val outputCol: Param[Array[String]] = new Param[Array[String]](this, "outputCol", "output columns name")

  final def getOutputCol: Array[String] = $(outputCol)

}
