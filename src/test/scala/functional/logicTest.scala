package functional

import org.scalatest.FlatSpec
import org.apache.spark.sql.Row
import testData.testData.{rawArray, rawDF, rawRDD, rawSeqRow, spark}
import logic.solutionStyle.{solution1_spark, solution2_sparkSQL, solution3_standard, solution4_rdd, solution5_recursion}

class logicTest extends FlatSpec {
    "solution1" should "return rows where values have odd counts and are unique per key" in {
      val sol1 = new solution1_spark
      val uniqueOdds = sol1.solution1(rawDF, spark)
      assert(uniqueOdds.collect() === Array(Row(1, 2)))
    }

  "solution2" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = solution2_sparkSQL.solution2(rawDF)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution3" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = solution3_standard.solution3(rawSeqRow)
    assert(uniqueOdds === Seq(Row(1, 2)))
  }

  "solution4" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = solution4_rdd.solution4(rawRDD)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution5" should "return rows where values have odd counts and are unique per key" in {
    val uniqueOdds = solution5_recursion.solution5(rawArray)
    assert(uniqueOdds === Array(Row(1, 2)))
  }

}