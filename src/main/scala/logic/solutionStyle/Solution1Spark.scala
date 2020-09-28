package logic.solutionStyle

import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Solution1Spark {

  def countFilterOddValues(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val counts= df.groupBy("key", "value").count().as("counts")
    val countsOdd = counts.filter($"count" % 2 =!=0).drop("count")
    countsOdd
  }

  def filterUniquelyOdd(df: DataFrame): DataFrame = {
    val uniqueOdd = df.killDuplicates("key")
    uniqueOdd
  }

  def solution1(df: DataFrame, spark: SparkSession): DataFrame = {
    val odd = countFilterOddValues(df, spark)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
