package logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import solution2_sparkSQL.solution2
import solution3_standard.solution3
import solution4_rdd.solution4
import solution5_recursion.solution5

object solution {

  abstract class solution[A] {
    def FindUniqueOdd(data: A): A
    def throwException: Exception
  }

  implicit val dataframeSolution: solution[DataFrame] = new solution[DataFrame] {
    def FindUniqueOdd(data: DataFrame):DataFrame = solution2(data)
    def throwException = throw new Exception("Empty list of DataFrame provided")
  }

  implicit val seqRowSolution: solution[Seq[Row]] = new solution[Seq[Row]] {
    def FindUniqueOdd(data: Seq[Row]): Seq[Row]= solution3(data)
    def throwException = throw new Exception("Empty list of Seq[Row] provided")
  }

  implicit val rddSolution: solution[RDD[Row]] = new solution[RDD[Row]] {
    def FindUniqueOdd(data: RDD[Row]): RDD[Row]= solution4(data)
    def throwException = throw new Exception("Empty list of RDD[Row] provided")
  }

  implicit val arrayRowSolution: solution[Array[Row]] = new solution[Array[Row]] {
    def FindUniqueOdd(data: Array[Row]): Array[Row]= solution5(data)
    def throwException = throw new Exception("Empty list of Array[Row] provided")
  }

  def solve[A](allData: List[A])(implicit monoid_input: solution[A]): Option[List[A]] = {
    try Some(allData.map(x => monoid_input.FindUniqueOdd(x)))
    catch {
      case e: Exception => None
    }
  }
}