import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import org.apache.spark.sql.Row
import dataHandlers.customSchema
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}



object transformFunctions {

  def solution1(df: DataFrame, spark: SparkSession): DataFrame = {
    /** Spark Dataframe method calls */
    import spark.implicits._

    // group by values in both column and return counts
    val df2 = df.groupBy("KEY", "VALUE").count().as("counts")
    
    // filter for odd numbers
    val df3 = df2.filter($"count" % 2 =!=0).drop("count") //.filter($"count" > 1).drop("count")

    // If there are more than 2 unique odd numbers
    val df4 = df3.killDuplicates("KEY")

    df4

  }


  def solution2(df: DataFrame, spark: SparkSession): DataFrame = {
    /** SQL Approach */

    df.createOrReplaceTempView("tab")
    val df2 = df.sqlContext
      .sql("SELECT KEY, VALUE, COUNT(*) as distinctCounts FROM tab GROUP BY KEY, VALUE")

    df2.createOrReplaceTempView("tab2")
    val df3 = df2.sqlContext
      .sql("SELECT KEY, VALUE FROM tab2 WHERE MOD (distinctCounts, 2) != 0 ") // this finds off numbers in randomCol1 problem -> WHEN WE replace expression with count(1), sql thinks it's a count - need to find ways to change name of count(1)

    val df4 = df3.killDuplicates("KEY")
    df4
  }

  def solution3(df:DataFrame, spark: SparkSession): DataFrame = {
    /** standard scala approach */

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

    // count each values per key and filter odd counts out...
    val countValPerKey = arrayData.map(x => (x._1, x._2.toList.groupBy(identity).mapValues(_.size).filter(x=> isOdd(x._1, x._2))))

    // filter out cases where a SEVERAL values occurs an odd number of times per key
    val oddMap = countValPerKey.filter(x => x._2.size == 1)

    // convert Map back into Spark Dataframe
    val uniquelyOddRows = oddMap.map( {case (key, value) => Row(key, value.keys.head)}).toSeq
    val rdd = spark.sparkContext.parallelize(uniquelyOddRows)
    val uniquelyOdd_df = spark.createDataFrame(rdd, customSchema)
    uniquelyOdd_df

  }


  def solution4(df: DataFrame, spark: SparkSession): DataFrame = {
    /** spark RDD Approach */
    val rdd = df.rdd


    val rdd2 = rdd.map(s => (s, 1))
    val counts = rdd2.reduceByKey((a, b) => a + b)
    counts.cache()

    val filtered = counts.filter(x => x._2 % 2 != 0)
    val rdd3 = filtered.map(s => s._1).groupBy(x => x.get(0))
    rdd3.cache()

    val rdd4 = rdd3.filter(x => x._2.toList.length == 1).map(x => x._2.head)


    val customSchema = StructType(Array(
      StructField("KEY", IntegerType, true),
      StructField("VALUE", IntegerType, true)
    ))
    val df_output = spark.createDataFrame(rdd4, customSchema)

    df_output
  }

}