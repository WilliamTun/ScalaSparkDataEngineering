package functional


import org.scalatest.WordSpec
import org.apache.spark.sql.Row
import data.KeyVal
import logic.Solution.solve
import testData.TestData.{rawDF, rawRDD, rawSeqRow, rawIterable}

class SolutionTest extends WordSpec {
  "solve()" when {

    "when given a DataFrame " should {
      "implicitly detect DataFrame and apply solution logic" in {
        val wrappedResult = solve(List(rawDF))
        val result = wrappedResult.head
        assert(result.collect() === Array(Row(1, 2)))
      }
    }


    "when given RDD[Row] " should {
      "implicitly detect RDD[Row] and apply solve" in {
        val wrappedResult = solve(List(rawRDD))
        val result = wrappedResult.head
        assert(result.collect() === Array(KeyVal(1, 2)))
      }
    }

    "when given Array[Row] " should {
      "implicitly detect Array[Row] and apply solve" in {
        val wrappedResult = solve(List(rawIterable))
        val result = wrappedResult.head
        assert(result === Array(KeyVal(1, 2)))
      }
    }

    "when given Seq[Row] " should {
      "implicitly detect Seq[Row] and apply solve" in {
        val wrappedResult = solve(List(rawSeqRow))
        val result = wrappedResult.head
        assert(result === Array(KeyVal(1, 2)))
      }
    }
  }
}
