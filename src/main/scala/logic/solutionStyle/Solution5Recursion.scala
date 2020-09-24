package logic.solutionStyle

import org.apache.spark.sql.Row

import scala.annotation.tailrec

object Solution5Recursion {

  @tailrec
  private def countFilterOddValues(ar: Array[Row], countMap: Map[Row, Int]): Iterable[Row] = {
    if (ar.isEmpty) {
      countMap.filter(x => x._2 % 2 != 0).keys
    } else {
      val currentKeyVal = ar.head
      if (countMap.contains(currentKeyVal)) {
        val newCount = countMap(currentKeyVal) + 1
        val newMap = countMap ++ Map(currentKeyVal -> newCount)
        countFilterOddValues(ar.tail, newMap)
      } else {
        val newMap = countMap ++ Map(currentKeyVal -> 1)
        countFilterOddValues(ar.tail, newMap)
      }
    }
  }

  @tailrec
  private def filterUniquelyOdd(ar: Iterable[Row], keyRowMap: Map[Int, Iterable[Row]]): Iterable[Row] = {
    if (ar.isEmpty) {
      keyRowMap.filter(x => x._2.toList.length == 1).map(x=>x._2.head)
    } else {
      val currentRow = ar.head
      val currentKey = currentRow(0).asInstanceOf[Int]
      if (keyRowMap.contains(currentKey)) {
        val row = keyRowMap(currentKey)
        //val row2 = ar.head
        val newIterableRow = row ++ Iterator(currentRow)
        val newMap = keyRowMap ++ Map(currentKey -> newIterableRow)
        filterUniquelyOdd(ar.tail, newMap)
      } else {
        val newMap = keyRowMap ++ Map(currentKey -> Iterable(currentRow))
        filterUniquelyOdd(ar.tail, newMap)
      }
    }
  }


  /* Note.
  Simpler non-recursive solution to FilterUniquelyOdd:

  val groupedByKey = counts_filtered.groupBy(x => x.get(0))
  val uniqueKeyVal = groupedByKey.filter(x => x._2.toList.length == 1).values
  uniqueKeyVal.foreach(x => println(x))
   */


  def solution5(arrayData: Array[Row]): Array[Row] = {
    val counts_filtered = countFilterOddValues(arrayData, Map())
    val uniquelyOddRows = filterUniquelyOdd(counts_filtered,  Map())
    uniquelyOddRows.toArray
  }

}
