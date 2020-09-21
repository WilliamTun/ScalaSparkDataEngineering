package logic
import data.dataHandlers.customSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.annotation.tailrec

object solution5_recursion {

  @tailrec
  private def CountFilterOddValues(ar: Array[Row], countMap: Map[Row, Int]): Iterable[Row] = {
    if (ar.isEmpty) {
      countMap.filter(x => x._2 % 2 != 0).keys
    } else {
      val currentKeyVal = ar.head
      if (countMap.contains(currentKeyVal)) {
        val newCount = countMap(currentKeyVal) + 1
        val newMap = countMap ++ Map(currentKeyVal -> newCount)
        CountFilterOddValues(ar.tail, newMap)
      } else {
        val newMap = countMap ++ Map(currentKeyVal -> 1)
        CountFilterOddValues(ar.tail, newMap)
      }
    }
  }

  @tailrec
  private def FilterUniquelyOdd(ar: Iterable[Row], keyRowMap: Map[Int, Iterable[Row]]): Iterable[Row] = {
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
        FilterUniquelyOdd(ar.tail, newMap)
      } else {
        val newMap = keyRowMap ++ Map(currentKey -> Iterable(currentRow))
        FilterUniquelyOdd(ar.tail, newMap)
      }
    }
  }


  /* Note.
  Simpler non-recursive solution to FilterUniquelyOdd:

  val groupedByKey = counts_filtered.groupBy(x => x.get(0))
  val uniqueKeyVal = groupedByKey.filter(x => x._2.toList.length == 1).values
  uniqueKeyVal.foreach(x => println(x))
   */


  def solution5(df: DataFrame, spark: SparkSession): DataFrame = {
    val arrayData = df.collect()
    val counts_filtered = CountFilterOddValues(arrayData, Map())
    val uniquelyOddRows = FilterUniquelyOdd(counts_filtered,  Map())

    val rdd = spark.sparkContext.parallelize(uniquelyOddRows.toSeq)
    val uniquelyOdd_df = spark.createDataFrame(rdd, customSchema)
    uniquelyOdd_df
  }

}




