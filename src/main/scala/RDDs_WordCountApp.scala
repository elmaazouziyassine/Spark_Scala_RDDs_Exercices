import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_WordCountApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)   // Level.ERROR

    // Create the entry point to SparkContext : sc
    val conf = new SparkConf().setMaster("local[*]").setAppName("Word Count Application using RDDs")
    val sc = new SparkContext(conf)

    // Load data into an RDD
    val lines = sc.textFile("src/datasets/article")

    // Transform data
    val words = lines.flatMap(line => line.split(" "))
    val wordsInPairs = words.map(word => (word,1))
    val countWords = wordsInPairs.reduceByKey(_+_)  // .reduceByKey((a,b) => (a+b))

    // Show results
    countWords.collect().foreach(println)

    // Save results
    countWords.saveAsTextFile("src/output_results/wordCount_output")

  }
}
