package unit

import data.dataHandlers.ReadData
import org.apache.spark.sql.{Row, SparkSession}
//import org.scalatest.FlatSpec
import org.scalatest.FunSpec

class dataHandlersTest extends FunSpec {

  val spark = SparkSession.builder
    .appName("SparkSessionExample")
    .master("local[4]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()


  describe("ReadData()") {
    describe("read CSV files into spark dataframe") {
      it("should handle integer values correctly") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/csv/dataTest.csv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.collect() === Array(Row(1, 3), Row(1, 3), Row(1, 2), Row(2, 5)))
      }
      it("should impute zero into empty values") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/csv/zeroTest.csv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.collect() === Array(Row(1, 0)))
      }
      it("should impose KEY and VALUE as headers") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/csv/headerTest.csv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.columns === Array("KEY", "VALUE"))
      }
    }

    describe("read TSV files into spark dataframe") {
      it ("should handle integer values correctly") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/tsv/dataTest.tsv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.collect() === Array(Row(1, 3), Row(1, 3), Row(1, 2), Row(2, 5)))
      }
      it("should impute zero into empty values") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/tsv/zeroTest.tsv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.collect() === Array(Row(1, 0)))
      }
      it("should impose KEY and VALUE as headers") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/tsv/headerTest.tsv"
        val format = inputPath.takeRight(3)
        val rawDF = ReadData(spark, inputPath, format)
        assert(rawDF.columns === Array("KEY", "VALUE"))
      }
    }
  }
}
