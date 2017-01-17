package transformers

import org.apache.spark.ml.param.Param

/**
  * Created by faiaz on 15.01.17.
  */
trait SingleTransformer extends CustomTransformer {

  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)

  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  setDefault(outputCol, uid + "__output")

  final def getOutputCol: String = $(outputCol)

}
