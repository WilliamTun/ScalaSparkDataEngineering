package unit


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FunSpec, PrivateMethodTester}
import testData.testData.{rawDF, rawRDD, rawSeqRow, rawIterable, spark, oddMap, oddRDD, oddIterable}
import logic.{solution1_spark, solution2_sparkSQL, solution3_standard, solution4_rdd, solution5_recursion}

class logicTest extends FunSpec with PrivateMethodTester {

  describe("solution1_spark") {
    val sol1 = new solution1_spark
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val decorateToDataFrame = PrivateMethod[DataFrame]('CountFilterOddValues)
        val oddCounts = sol1 invokePrivate decorateToDataFrame(rawDF, spark)
        oddCounts.show()
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        // room to improve
        val uniqueOdds = sol1.solution1(rawDF, spark)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution2_sparkSQL") {
    val sol = solution2_sparkSQL
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val decorateToDataFrame2 = PrivateMethod[DataFrame]('CountFilterOddValues)
        val oddCounts = sol invokePrivate decorateToDataFrame2(rawDF)
        assert(oddCounts.collect() === Array(Row(1, 2), Row(2, 5), Row(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        // room to improve -> learn how to put in protected
        val uniqueOdds = sol.solution2(rawDF)
        assert(uniqueOdds.collect() === Array(Row(1, 2)))
      }
    }
  }

  describe("solution3_standard") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val decorateToMap = PrivateMethod[Map[Int, Map[Int, Int]]]('CountFilterOddValues)
        val oddCounts = solution3_standard invokePrivate decorateToMap(rawSeqRow)
        val oddMap = oddCounts.map(x => x._1 -> x._2.keys.sliding(1).map(y => y.head).toList)
        val oddSeq = oddMap.toSeq.flatMap { case (key, list) => list.map(key -> _) }
        assert(oddSeq === Seq((1, 2), (2, 5), (2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val decorateToSeqRow = PrivateMethod[Seq[Row]]('FilterUniquelyOdd)
        val uniqueOddRow = solution3_standard invokePrivate decorateToSeqRow(oddMap)
        assert(uniqueOddRow === List(Row(1, 2)))
      }
    }
  }


  describe("solution4_rdd") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val decorateToRDD = PrivateMethod[RDD[(Row, Int)]]('CountFilterOddValues)
        val oddCounts = solution4_rdd invokePrivate decorateToRDD(rawRDD)
        val compare = Set((Row(1, 2), 1), (Row(2, 5), 1), (Row(2, 6), 1))
        assert(oddCounts.collect().toSet.diff(compare).isEmpty)
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val decorateToDataFrame = PrivateMethod[RDD[Row]]('FilterUniquelyOdd)
        val uniqueOdd = solution4_rdd invokePrivate decorateToDataFrame(oddRDD)
        val out = uniqueOdd.collect()
        assert(out === Array(Row(1, 2)))
      }
    }
  }

  describe("solution5_recursion") {
    describe("CountFilterOddValues()") {
      it("filter for keys with VALUES that have odd counts") {
        val decorateToIterable = PrivateMethod[Iterable[Row]]('CountFilterOddValues)
        val odd = solution5_recursion invokePrivate decorateToIterable(rawIterable, Map())
        assert(odd === Set(Row(1, 2), Row(2, 5), Row(2, 6)))
      }
    }
    describe("FilterUniquelyOdd()") {
      it("filter for UNIQUE KEYS with odd counts") {
        val decorateToIterable = PrivateMethod[Iterable[Row]]('FilterUniquelyOdd)
        val uniqueOdd = solution5_recursion invokePrivate decorateToIterable(oddIterable, Map())
        assert(uniqueOdd === List(Row(1, 2)))
      }
    }
  }

}


