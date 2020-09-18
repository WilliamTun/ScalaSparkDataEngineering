package logic
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import data.dataHandlers.customSchema


object solution4_rdd {

  private def CountFilterOddValues(df: DataFrame): RDD[(Row, Int)] = {
    val rdd = df.rdd
    val rdd2 = rdd.map(s => (s, 1))
    val counts = rdd2.reduceByKey((a, b) => a + b)
    counts.cache()

    val countsOdd = counts.filter(x => x._2 % 2 != 0)
    countsOdd
  }

  private def FilterUniquelyOdd(countsOdd:  RDD[(Row, Int)]): RDD[Row] = {
    val countsOddGrouped = countsOdd.map(s => s._1).groupBy(x => x.get(0))
    countsOddGrouped.cache()

    val uniqueOdd = countsOddGrouped.filter(x => x._2.toList.length == 1).map(x => x._2.head)
    uniqueOdd
  }


  def solution4(df: DataFrame, spark: SparkSession): DataFrame = {
    val odd = CountFilterOddValues(df)
    val uniqOdd = FilterUniquelyOdd(odd)
    val df_output = spark.createDataFrame(uniqOdd, customSchema)
    df_output
  }

}
