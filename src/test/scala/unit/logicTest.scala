package unit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FunSpec, PrivateMethodTester}
import testData.testData.{rawDF, spark}



class logicTest extends FunSpec with PrivateMethodTester {

  describe("solution1_spark") {
    describe("CountFilterOddValues()") {
      import logic.solution1_spark
      val sol1 = new solution1_spark

      it("filter for keys with VALUES that have odd counts") {
        val decorateToDataFrame = PrivateMethod[DataFrame]('CountFilterOddValues)
        val oddCounts = sol1 invokePrivate decorateToDataFrame(rawDF, spark)
        oddCounts.show()
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2,6)))
      }

      it("filter for UNIQUE KEYS with odd counts") {
        // room to improve
        val uniqueOdds = sol1.solution1(rawDF, spark)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution2_sparkSQL") {
    describe("CountFilterOddValues()") {
      import logic.solution2_sparkSQL
      val sol = solution2_sparkSQL

      it("filter for keys with VALUES that have odd counts") {
        val decorateToDataFrame2 = PrivateMethod[DataFrame]('CountFilterOddValues)
        val oddCounts = sol invokePrivate decorateToDataFrame2(rawDF)
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2,6)))
      }

      it("filter for UNIQUE KEYS with odd counts") {
        // room to improve -> learn how to put in protected
        val uniqueOdds = sol.solution2(rawDF)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution3_standard") {
    describe("CountFilterOddValues()") {
      import logic.solution3_standard
      it("filter for keys with VALUES that have odd counts") {

        val decorateToDataFrame = PrivateMethod[Map[Int, Map[Int, Int]]]('CountFilterOddValues)
        val oddCounts = solution3_standard invokePrivate decorateToDataFrame(rawDF)
        val oddMap = oddCounts.map(x => x._1 -> x._2.keys.sliding(1).map(y => y.head).toList)
        val oddSeq = oddMap.toSeq.flatMap { case (key, list) => list.map(key -> _) }
        assert(oddSeq === Seq((1, 2), (2, 5), (2, 6)))
      }

      it("filter for UNIQUE KEYS with odd counts") {
        import testData.testData.oddMap
        val decorateToDataFrame = PrivateMethod[Seq[Row]]('FilterUniquelyOdd)
        val uniqueOddRow = solution3_standard invokePrivate decorateToDataFrame(oddMap)
        assert(uniqueOddRow === List(Row(1,2)))
      }
    }
  }

  describe("solution4_rdd") {
    describe("CountFilterOddValues()") {
      import logic.solution4_rdd
      it("filter for keys with VALUES that have odd counts") {
        val decorateToDataFrame = PrivateMethod[RDD[(Row, Int)]]('CountFilterOddValues)
        val oddCounts = solution4_rdd invokePrivate decorateToDataFrame(rawDF)
        val compare = Set((Row(1, 2), 1), (Row(2, 5), 1), (Row(2, 6), 1))
        assert(oddCounts.collect().toSet.diff(compare).isEmpty)
      }
      it("filter for UNIQUE KEYS with odd counts") {
        import testData.testData.rawRdd
        val decorateToDataFrame = PrivateMethod[RDD[Row]]('FilterUniquelyOdd)
        val uniqueOdd = solution4_rdd invokePrivate decorateToDataFrame(rawRdd)
        val out = uniqueOdd.collect()
        assert(out === Array(Row(1,2)))
      }
    }
  }

  describe("solution5_recursion") {
    describe("CountFilterOddValues()") {
      import logic.solution5_recursion

      it("filter for keys with VALUES that have odd counts") {
        import testData.testData.rawIterable
        val decorateToDataFrame = PrivateMethod[Iterable[Row]]('CountFilterOddValues)
        val odd = solution5_recursion invokePrivate decorateToDataFrame(rawIterable, Map())
        assert(odd === Set(Row(1, 2), Row(2, 5), Row(2,6)))
      }

      it("filter for UNIQUE KEYS with odd counts") {
        import testData.testData.oddIterable
        val decorateToDataFrame2 = PrivateMethod[Iterable[Row]]('FilterUniquelyOdd)
        val uniqueOdd = solution5_recursion invokePrivate decorateToDataFrame2(oddIterable, Map())
        assert(uniqueOdd === List(Row(1, 2)))
      }
    }
  }


}
