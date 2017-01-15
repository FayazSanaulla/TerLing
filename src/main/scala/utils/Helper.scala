package utils

import org.apache.spark.sql.DataFrame

/**
  * Created by faiaz on 13.01.17.
  */
trait Helper {
  final def print(df: DataFrame): Unit = df.collect().foreach(println)

  final def loadResources(path: String): Array[String] = {
    val is = getClass.getResourceAsStream(path)
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
