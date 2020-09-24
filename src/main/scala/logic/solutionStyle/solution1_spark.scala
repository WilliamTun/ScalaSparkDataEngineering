package logic.solutionStyle

import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import org.apache.spark.sql.{DataFrame, SparkSession}

class solution1_spark {

  private def CountFilterOddValues(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val counts= df.groupBy("KEY", "VALUE").count().as("counts")
    val countsOdd = counts.filter($"count" % 2 =!=0).drop("count")
    countsOdd
  }

  protected def FilterUniquelyOdd(df: DataFrame): DataFrame = {
    val uniqueOdd = df.killDuplicates("KEY")
    uniqueOdd
  }

  def solution1(df: DataFrame, spark: SparkSession): DataFrame = {
    val odd = CountFilterOddValues(df, spark)
    val uniqOdd = FilterUniquelyOdd(odd)
    uniqOdd
  }

}
