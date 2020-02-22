import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_LogMining {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF) // Level.ERROR

    // Create the entry point to SparkContext : sc
    val conf = new SparkConf().setMaster("local[*]").setAppName("Log Mining")
    val sc = new SparkContext(conf)

    // Load data
    val lines = sc.textFile("src/datasets/log")
    //lines.collect().foreach(println)

    // Find ERROR lines
    val errors = lines.filter(_.startsWith("ERROR"))
    //errors.collect().foreach(println)

    val errorsPaired = errors.map(_.split(":"))
    //errorsPaired.collect().foreach(println)

    // Extract messages
    val messages = errorsPaired.map(x => x(1))
    messages.collect().foreach(println)
    messages.cache()

    // Count messages that contains "first"
    print(messages.filter(_.contains("first")).count())
  }
}