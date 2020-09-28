package logic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import logic.solutionStyle.Solution2SparkSQL.solution2
import logic.solutionStyle.Solution3Standard.solution3
import logic.solutionStyle.Solution4Rdd.solution4
import logic.solutionStyle.Solution5Recursion.solution5
import data.DataWriter.{writeArray, writeDataFrame, writeRDD, writeSeq, zip}
import java.io.File
import data.KeyVal

object Solution {

  abstract class solution[A] {
    def findUniqueOdd(data: A): A
    def writeData(df: A, path:String): Unit
    def throwException: Exception
  }

  implicit val dataframeSolution: solution[DataFrame] = new solution[DataFrame] {
    def findUniqueOdd(data: DataFrame):DataFrame = solution2(data)
    //resourcePath: String, output: List[DataFrame], spark: SparkSession
    def writeData(output: DataFrame, path: String) = writeDataFrame(output, path)
    def throwException = throw new Exception("Empty list of DataFrame provided")
  }

  implicit val seqSolution: solution[Seq[KeyVal]] = new solution[Seq[KeyVal]] {
    def findUniqueOdd(data: Seq[KeyVal]): Seq[KeyVal]= solution3(data)
    def writeData(output: Seq[KeyVal], path: String) = writeSeq(output, path)
    def throwException = throw new Exception("Empty list of Seq[Row] provided")
  }

  implicit val rddSolution: solution[RDD[KeyVal]] = new solution[RDD[KeyVal]] {
    def findUniqueOdd(data: RDD[KeyVal]): RDD[KeyVal]= solution4(data)
    def writeData(output: RDD[KeyVal], path: String) = writeRDD(output, path)
    def throwException = throw new Exception("Empty list of RDD[Row] provided")
  }

  implicit val arraySolution: solution[Array[KeyVal]] = new solution[Array[KeyVal]] {
    def findUniqueOdd(data: Array[KeyVal]): Array[KeyVal]= solution5(data)
    def writeData(output: Array[KeyVal], path: String) = writeArray(output, path)
    def throwException = throw new Exception("Empty list of Array[Row] provided")
  }

  def solve[A : solution](allData: List[A]): List[A] = {
    val result = allData map {
      case x1: DataFrame => dataframeSolution.findUniqueOdd(x1)
      case x2: Seq[KeyVal] => seqSolution.findUniqueOdd(x2)
      case x3: RDD[KeyVal] => rddSolution.findUniqueOdd(x3)
      case x4: Array[KeyVal] => arraySolution.findUniqueOdd(x4)
    }
    result.map(x => x.asInstanceOf[A])
  }

  def write[A: solution](output: List[A], path: String): Unit = {
    val folderPath = path + "outputFolder"
    val dir = new File(folderPath)
    dir.mkdir()

    val zipNameResult = zip(path, output)

    zipNameResult foreach { x => x._2 match {
        case out: DataFrame => dataframeSolution.writeData(out, x._1)
        case out: Seq[KeyVal] => seqSolution.writeData(out, x._1)
        case out: RDD[KeyVal] => rddSolution.writeData(out, x._1)
        case out: Array[KeyVal] => arraySolution.writeData(out, x._1)
      }
    }
    Unit
  }
}