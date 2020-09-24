package logic.solutionStyle

import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Solution1Spark {

  private def countFilterOddValues(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val counts= df.groupBy("KEY", "VALUE").count().as("counts")
    val countsOdd = counts.filter($"count" % 2 =!=0).drop("count")
    countsOdd
  }

  protected def filterUniquelyOdd(df: DataFrame): DataFrame = {
    val uniqueOdd = df.killDuplicates("KEY")
    uniqueOdd
  }

  def solution1(df: DataFrame, spark: SparkSession): DataFrame = {
    val odd = countFilterOddValues(df, spark)
    val uniqOdd = filterUniquelyOdd(odd)
    uniqOdd
  }

}
