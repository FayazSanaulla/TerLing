package transformers

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * Created by faiaz on 15.01.17.
  */
trait MultipleTransformer extends Params {

  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)

  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCol", "output columns name")
  setDefault(outputCols, Array("word", "pair"))

  final def getOutputCols: Array[String] = $(outputCols)
}
