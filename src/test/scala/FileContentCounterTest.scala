import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class FileContentCounterTest extends FunSuite with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    //setting 2 worker threads
    sc = new SparkContext(new SparkConf().setAppName("FileContentCounterTest").setMaster("local[2]"))
  }

  after {
    //Spark does not support two spark contexts running concurrently in the same program. So stopping context after each test.
    if (sc != null) {
      sc.stop()
    }
  }

  test("verifying line/word counts") {

    val pc = new FileContentCounter(sc)(getClass.getResource("sample_file.txt").getFile)
    assert(pc.lineCount() == 4, "line count mismatch")

    val testWord: String = "hare"
    assert(pc.wordCountMap().get(testWord) == Some(4), s"word($testWord) count mismatch")
  }


}
