package unit

import data.DataReader.readFile
import data.KeyVal
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, PrivateMethodTester}

class DataReaderTest extends FunSpec with PrivateMethodTester {

  val spark = SparkSession.builder
    .appName("Test DataHandlers")
    .master("local[4]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()


  describe("ReadData()") {
    describe("read CSV files into spark dataframe") {
      it("should handle integer values correctly") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/csv/dataTest.csv"
        val raw = readFile(inputPath)
        assert(raw === Stream(KeyVal(1, 3), KeyVal(1, 3), KeyVal(1, 2), KeyVal(2, 5)))
      }
      it("should impute zero into empty values") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/csv/zeroTest.csv"
        val raw = readFile(inputPath)
        assert(raw === Stream(KeyVal(1, 0)))
      }
    }

    describe("read TSV files into spark dataframe") {
      it ("should handle integer values correctly") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/tsv/dataTest.tsv"
        val raw = readFile(inputPath)
        assert(raw === Stream(KeyVal(1, 3), KeyVal(1, 3), KeyVal(1, 2), KeyVal(2, 5)))
      }
      it("should impute zero into empty values") {
        val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/tests/tsv/zeroTest.tsv"
        val raw = readFile(inputPath)
        assert(raw === Stream(KeyVal(1, 0)))
      }

    }
  }
}