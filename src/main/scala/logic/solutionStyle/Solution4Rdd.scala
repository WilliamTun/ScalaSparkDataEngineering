package logic.solutionStyle

import data.KeyVal
import org.apache.spark.rdd.RDD

object Solution4Rdd {

  def countFilterOddValues(rd: RDD[KeyVal]): RDD[(KeyVal, Int)] = {
    val rdd = rd

    val rdd2 = rdd.map(s => (s, 1))
    val counts = rdd2.reduceByKey((a, b) => a + b)
    counts.cache()

    val countsOdd = counts.filter(x => x._2 % 2 != 0)
    countsOdd
  }

  def filterUniquelyOdd(countsOdd:  RDD[(KeyVal, Int)]): RDD[KeyVal] = {
    val countsOddGrouped = countsOdd.map(s => s._1).groupBy(x => x.key)
    countsOddGrouped.cache()

    val uniqueOdd = countsOddGrouped.filter(x => x._2.toList.length == 1).map(x => x._2.head)
    uniqueOdd
  }

  def solution4(data: RDD[KeyVal]): RDD[KeyVal] = {
    val odd = countFilterOddValues(data)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
