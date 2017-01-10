package transformers.adapters

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import transformers.CustomTransformer

/**
  * Created by faiaz on 09.01.17.
  */
class LogisticRegressionAdapter(override val uid: String = Identifiable.randomUID("logisticAdapter"))
  extends Transformer
    with CustomTransformer {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}
