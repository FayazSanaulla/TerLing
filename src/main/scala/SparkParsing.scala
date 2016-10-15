import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by faiaz on 14.10.16.
  */
class SparkParsing {

  private val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")

  private val sc = new SparkContext(conf)

  private var classpath = ""

  def setFile(classPath: String) = classpath = classPath

  def count = {
    sc.textFile(classpath)
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }
}

