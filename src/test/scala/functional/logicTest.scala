package functional

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import testData.testData.{rawDF, spark}

class logicTest extends FlatSpec {
    "solution1" should "return rows where values have odd counts and are unique per key" in {
      import logic.solution1_spark
      val sol1 = new solution1_spark
      val uniqueOdds = sol1.solution1(rawDF, spark)
      assert(uniqueOdds.collect() === Array(Row(1, 2)))
    }

  "solution2" should "return rows where values have odd counts and are unique per key" in {
    import logic.solution2_sparkSQL
    val uniqueOdds = solution2_sparkSQL.solution2(rawDF)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution3" should "return rows where values have odd counts and are unique per key" in {
    import logic.solution3_standard

    val uniqueOdds = solution3_standard.solution3(rawDF, spark)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution4" should "return rows where values have odd counts and are unique per key" in {
    import logic.solution4_rdd
    val uniqueOdds = solution4_rdd.solution4(rawDF, spark)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }

  "solution5" should "return rows where values have odd counts and are unique per key" in {
    import logic.solution5_recursion
    val uniqueOdds = solution5_recursion.solution5(rawDF, spark)
    assert(uniqueOdds.collect() === Array(Row(1, 2)))
  }
}
