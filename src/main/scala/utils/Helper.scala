package utils

import org.apache.spark.sql.DataFrame

/**
  * Created by faiaz on 13.01.17.
  */
object Helper {
  def print(df: DataFrame): Unit = df.collect().foreach(println)
}
