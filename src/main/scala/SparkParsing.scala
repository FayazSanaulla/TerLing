import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by faiaz on 14.10.16.
  */
object SparkParsing extends App {

  val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("bigData")
    .set("spark.executor.memory", "1g")

  val sc = new SparkContext(conf)

  val file = sc.parallelize(Seq("Fayaz,", "Riaz,", "Anna"))

  val count = file.flatMap(line => line.split(","))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  count.foreach(println)
}

