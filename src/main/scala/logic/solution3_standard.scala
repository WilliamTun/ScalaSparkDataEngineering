package logic
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import data.dataHandlers.customSchema

object solution3_standard {

  private def CountFilterOddValues(df: DataFrame): Map[Int, Map[Int, Int]] = {
    val arrayData = df.collect().foldLeft(Map.empty[Int, Array[Int]]) {
      case (map, elem) =>
        val currentKey = elem.getInt(0)
        val currentVal = elem.getInt(1)
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

  private def FilterUniquelyOdd(countsOdd: Map[Int, Map[Int, Int]]): Seq[Row] = {
    val uniqueOdd = countsOdd.filter(x => x._2.size == 1)
    val uniqueOddRow = uniqueOdd.map( {case (key, value) => Row(key, value.keys.head)}).toSeq
    uniqueOddRow
  }


  def solution3(df:DataFrame, spark: SparkSession): DataFrame = {
    val odd = CountFilterOddValues(df)
    val uniqOdd = FilterUniquelyOdd(odd)

    val rdd = spark.sparkContext.parallelize(uniqOdd)
    val uniquelyOdd_df = spark.createDataFrame(rdd, customSchema)
    uniquelyOdd_df
  }

}