package logic.solutionStyle

import org.apache.spark.sql.DataFrame

object Solution2SparkSQL extends Solution1Spark {

   def countFilterOddValues(df: DataFrame): DataFrame = {
     df.createOrReplaceTempView("tab")
     val counts = df.sqlContext
      .sql("SELECT KEY, VALUE, COUNT(*) as distinctCounts FROM tab GROUP BY KEY, VALUE")

     counts.createOrReplaceTempView("tab2")
     val countsOdd = counts.sqlContext
      .sql("SELECT KEY, VALUE FROM tab2 WHERE MOD (distinctCounts, 2) != 0 ") // this finds off numbers in randomCol1 problem -> WHEN WE replace expression with count(1), sql thinks it's a count - need to find ways to change name of count(1)

     countsOdd
   }

  def solution2(df: DataFrame): DataFrame = {
    val odd = countFilterOddValues(df)
    val uniOdd = filterUniquelyOdd(odd)
    uniOdd
  }

}
