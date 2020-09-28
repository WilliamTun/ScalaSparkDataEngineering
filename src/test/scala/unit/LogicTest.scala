package unit

import org.apache.spark.sql.Row
import org.scalatest.{FunSpec, PrivateMethodTester}
import testData.TestData.{oddIterable, oddMap, oddRDD, oddDF, rawDF, rawIterable, rawRDD, rawSeqRow, spark}
import logic.solutionStyle.{Solution1Spark, Solution2SparkSQL, Solution3Standard, Solution4Rdd, Solution5Recursion}
import data.KeyVal

class LogicTest extends FunSpec with PrivateMethodTester {

  describe("solution1_spark") {
    val sol1 = new Solution1Spark
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val oddCounts = sol1.countFilterOddValues(rawDF, spark)
        oddCounts.show()
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val uniqueOdds = sol1.filterUniquelyOdd(oddDF)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution2_sparkSQL") {
    val sol = Solution2SparkSQL
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val oddCounts = sol.countFilterOddValues(rawDF)
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val uniqueOdds = sol.filterUniquelyOdd(oddDF)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution3_standard") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val oddCounts = Solution3Standard.countFilterOddValues(rawSeqRow)
        val oddMap = oddCounts.map(x => x._1 -> x._2.keys.sliding(1).map(y => y.head).toList)
        val oddSeq = oddMap.toSeq.flatMap { case (key, list) => list.map(key -> _) }
        assert(oddSeq === Seq((1, 2), (2, 5), (2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val uniqueOddRow = Solution3Standard.filterUniquelyOdd(oddMap)
        assert(uniqueOddRow === List(KeyVal(1, 2)))
      }
    }
  }


  describe("solution4_rdd") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val oddCounts = Solution4Rdd.countFilterOddValues(rawRDD)
        val compare = Set((KeyVal(1, 2), 1), (KeyVal(2, 5), 1), (KeyVal(2, 6), 1))
        assert(oddCounts.collect().toSet.diff(compare).isEmpty)
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val uniqueOdd = Solution4Rdd.filterUniquelyOdd(oddRDD)
        val out = uniqueOdd.collect()
        assert(out === Array(KeyVal(1, 2)))
      }
    }
  }

  describe("solution5_recursion") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val odd = Solution5Recursion.countFilterOddValues(rawIterable, Map())
        assert(odd === Set(KeyVal(1, 2), KeyVal(2, 5), KeyVal(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val uniqueOdd = Solution5Recursion.filterUniquelyOdd(oddIterable, Map())
        assert(uniqueOdd === List(KeyVal(1, 2)))
      }
    }
  }

}