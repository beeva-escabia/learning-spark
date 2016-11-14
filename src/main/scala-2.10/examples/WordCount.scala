package examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rubenescabia on 14/11/16.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val logFile = "/home/rubenescabia/apps/spark-2.0.1-bin-hadoop2.7/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()

    val words = logData.flatMap(line => line.split(" "))
    val counts = words.map(word => (word,1)).reduceByKey{case (x,y) => x + y}

    println("Word counts: " + counts)
  }

}
