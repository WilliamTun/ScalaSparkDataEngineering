package functional

import org.scalatest.FlatSpec
import org.apache.spark.sql.Row
import testData.TestData.{rawArray, rawDF, rawRDD, rawSeqRow, spark}
import logic.solutionStyle.{Solution1Spark, Solution2SparkSQL, Solution3Standard, Solution4Rdd, Solution5Recursion}
import data.KeyVal

class LogicTest extends FlatSpec {

  "solution1" should "return rows where values have odd counts and are unique per key" in {
    val sol = new Solution1Spark
    val uniqueOdds = sol.solution1(rawDF, spark)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution2" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = Solution2SparkSQL.solution2(rawDF)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution3" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = Solution3Standard.solution3(rawSeqRow)
    assert(uniqueOdds === Seq(KeyVal(1, 2)))
  }

  "solution4" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = Solution4Rdd.solution4(rawRDD)
    assert(uniqueOdds.collect() === Array(KeyVal(1, 2)))
  }

  "solution5" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = Solution5Recursion.solution5(rawArray)
    assert(uniqueOdds === Array(KeyVal(1, 2)))
  }

}
