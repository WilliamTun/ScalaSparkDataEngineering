package logic.solutionStyle

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Solution4Rdd {

  private def countFilterOddValues(rd: RDD[Row]): RDD[(Row, Int)] = {
    val rdd = rd

    val rdd2 = rdd.map(s => (s, 1))
    val counts = rdd2.reduceByKey((a, b) => a + b)
    counts.cache()

    val countsOdd = counts.filter(x => x._2 % 2 != 0)
    countsOdd
  }

  private def filterUniquelyOdd(countsOdd:  RDD[(Row, Int)]): RDD[Row] = {
    val countsOddGrouped = countsOdd.map(s => s._1).groupBy(x => x.get(0))
    countsOddGrouped.cache()

    val uniqueOdd = countsOddGrouped.filter(x => x._2.toList.length == 1).map(x => x._2.head)
    uniqueOdd
  }

  def solution4(df: RDD[Row]): RDD[Row] = {
    val odd = countFilterOddValues(df)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
