package pipeline.transformers

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by faiaz on 01.01.17.
  */
trait CustomTransformer extends Params {

  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)

  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  setDefault(outputCol, uid + "__output")

  final def getOutputCol: String = $(outputCol)
}
