package pipeline

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by faiaz on 30.12.16.
  */
class MyTransformer extends Transformer with DefaultParamsWritable{

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = _
}
