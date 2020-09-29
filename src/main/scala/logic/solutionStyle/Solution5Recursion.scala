package logic.solutionStyle

import data.KeyVal
import scala.annotation.tailrec

object Solution5Recursion {

  @tailrec
  def countFilterOddValues(ar: Array[KeyVal], countMap: Map[KeyVal, Int]): Iterable[KeyVal] = {
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
  def filterUniquelyOdd(ar: Iterable[KeyVal], keyRowMap: Map[Int, Iterable[KeyVal]]): Iterable[KeyVal] = {
    if (ar.isEmpty) {
      keyRowMap.filter(x => x._2.toList.length == 1).map(x=>x._2.head)
    } else {
      val currentRow = ar.head
      val currentKey = currentRow.key
      if (keyRowMap.contains(currentKey)) {
        val row = keyRowMap(currentKey)
        val newIterableRow = row ++ Iterator(currentRow)
        val newMap = keyRowMap ++ Map(currentKey -> newIterableRow)
        filterUniquelyOdd(ar.tail, newMap)
      } else {
        val newMap = keyRowMap ++ Map(currentKey -> Iterable(currentRow))
        filterUniquelyOdd(ar.tail, newMap)
      }
    }
  }

  def solution5(arrayData: Array[KeyVal]): Array[KeyVal] = {
    val counts_filtered = countFilterOddValues(arrayData, Map())
    val uniquelyOddRows = filterUniquelyOdd(counts_filtered,  Map())
    uniquelyOddRows.toArray
  }

}
