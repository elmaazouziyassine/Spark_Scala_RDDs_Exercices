import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDs_PiEstimation {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)   // Level.ERROR

    // Create the entry point to SparkContext : sc
    val conf = new SparkConf().setMaster("local[*]").setAppName("Word Count Application using RDDs")
    val sc = new SparkContext(conf)

    /* Implement the following algorithm :
    - pick random points in the unit square ((0, 0) to (1,1)) :  here NUMBER_OF_SAMPLES
    - see how many fall in the unit circle : logic inside the filter method
    - count the the fraction (countValidPoints/NUMBER_OF_SAMPLES) that should be π / 4
    */
    var NUMBER_OF_SAMPLES = 1000000
    val countValidPoints = sc.parallelize(0 to NUMBER_OF_SAMPLES).filter{ _ =>
      val a = math.random
      val b = math.random
      a*a + b*b < 1
    }.count()

    println(s"The value of PI is roughly ${4.0 * countValidPoints / NUMBER_OF_SAMPLES}")
  }
}
