import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * File Content counter
 *
 * A very basic example to get an example going with spark.
 * 
 */
class FileContentCounter(sc: SparkContext)(fileName: String) {

  val lines = sc.textFile(fileName)

  //caching in memory to use it across operations(lineCount/wordCountMap)
  lines.cache()

  /**
   * Counts the number of lines in a file
   *
   * @return
   */
  def lineCount(): Int = {
    lines.map(line => 1).reduce(_ + _)
  }

  /**
   * Counts the number of times each word occurs
   * in a file and retuns a map of word and the number
   * of times it occurs.
   *
   * @return
   */
  def wordCountMap(): Map[String, Int] = {

    lines.flatMap(line => line.split("\\W+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap
  }

}
