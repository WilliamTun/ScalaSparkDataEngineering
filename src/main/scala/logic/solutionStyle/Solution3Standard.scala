package logic.solutionStyle

import data.KeyVal

object Solution3Standard {

  def countFilterOddValues(df: Seq[KeyVal]): Map[Int, Map[Int, Int]] = {
    val arrayData = df.foldLeft(Map.empty[Int, Array[Int]]) {
      case (map, elem) =>
        val currentKey = elem.key
        val currentVal = elem.value
        if (map.keys.exists(_ == currentKey)) {
          val newVal: Array[Int] = map(currentKey) ++ Array(currentVal)
          map.updated(currentKey, newVal)
        } else {
          map + (currentKey -> Array(currentVal))
        }
    }

    val isOdd = (x: Int, y:Int) => y % 2 != 0
    val countsOdd = arrayData.map(x => (x._1, x._2.toList.groupBy(identity).mapValues(_.size).filter(x=> isOdd(x._1, x._2))))
    countsOdd
  }

  def filterUniquelyOdd(countsOdd: Map[Int, Map[Int, Int]]): Seq[KeyVal] = {
    val uniqueOdd = countsOdd.filter(x => x._2.size == 1)
    val uniqueOddRow = uniqueOdd.map( {case (key, value) => KeyVal(key, value.keys.head)}).toSeq
    uniqueOddRow
  }


  def solution3(data:Seq[KeyVal]): Seq[KeyVal] = {
    val odd = countFilterOddValues(data)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
