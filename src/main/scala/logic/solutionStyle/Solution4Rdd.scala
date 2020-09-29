package logic.solutionStyle

import data.KeyVal
import org.apache.spark.rdd.RDD

object Solution4Rdd {

  def countFilterOddValues(rdd: RDD[KeyVal]): RDD[(KeyVal, Int)] = {
    val rdd2 = rdd.map((_, 1))
    val counts = rdd2.reduceByKey(_ + _)
    counts.cache()

    val countsOdd = counts.filter(keyValCountTup => keyValCountTup._2 % 2 != 0)
    countsOdd
  }

  def filterUniquelyOdd(countsOdd:  RDD[(KeyVal, Int)]): RDD[KeyVal] = {
    val countsOddGrouped = countsOdd.map(_._1).groupBy(_.key)
    countsOddGrouped.cache()

    val uniqueOdd = countsOddGrouped
      .filter(keyIterTup => keyIterTup._2.toList.length == 1)
      .map(keyIterTup => keyIterTup._2.head)
    uniqueOdd
  }

  def solution4(data: RDD[KeyVal]): RDD[KeyVal] = {
    val odd = countFilterOddValues(data)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
