package utils

/**
  * Created by faiaz on 15.01.17.
  */
trait ResourceLoader {
  final def loadResources(path: String): Array[String] = {
    val is = getClass.getResourceAsStream(path)
    scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toArray
  }
}
