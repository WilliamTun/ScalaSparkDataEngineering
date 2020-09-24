package logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import logic.solutionStyle.Solution2SparkSQL.solution2
import logic.solutionStyle.Solution3Standard.solution3
import logic.solutionStyle.Solution4Rdd.solution4
import logic.solutionStyle.Solution5Recursion.solution5

object Solution {

  abstract class solution[A] {
    def findUniqueOdd(data: A): A
    def throwException: Exception
  }

  implicit val dataframeSolution: solution[DataFrame] = new solution[DataFrame] {
    def findUniqueOdd(data: DataFrame):DataFrame = solution2(data)
    def throwException = throw new Exception("Empty list of DataFrame provided")
  }

  implicit val seqRowSolution: solution[Seq[Row]] = new solution[Seq[Row]] {
    def findUniqueOdd(data: Seq[Row]): Seq[Row]= solution3(data)
    def throwException = throw new Exception("Empty list of Seq[Row] provided")
  }

  implicit val rddSolution: solution[RDD[Row]] = new solution[RDD[Row]] {
    def findUniqueOdd(data: RDD[Row]): RDD[Row]= solution4(data)
    def throwException = throw new Exception("Empty list of RDD[Row] provided")
  }

  implicit val arrayRowSolution: solution[Array[Row]] = new solution[Array[Row]] {
    def findUniqueOdd(data: Array[Row]): Array[Row]= solution5(data)
    def throwException = throw new Exception("Empty list of Array[Row] provided")
  }

  def solve[A](allData: List[A])(implicit monoid_input: solution[A]): Option[List[A]] = {
    try Some(allData.map(x => monoid_input.findUniqueOdd(x)))
    catch {
      case e: Exception => None
    }
  }
}