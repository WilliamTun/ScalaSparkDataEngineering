import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import org.apache.spark.sql.Row
import dataHandlers.customSchema
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import scala.annotation.tailrec

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

  def solution5(df: DataFrame, spark: SparkSession): DataFrame = {
    /** Tail Recursion Approach */

    val arrayData = df.collect()

    @tailrec
    def count_filter(ar: Array[Row], countMap: Map[Row, Int]): Iterable[Row] = {
      if (ar.isEmpty) {
        countMap.filter(x => x._2 % 2 != 0).keys
      } else {
        val currentKeyVal = ar.head
        if (countMap.contains(currentKeyVal)) {
          val newCount = countMap(currentKeyVal) + 1
          val newMap = countMap ++ Map(currentKeyVal -> newCount)
          count_filter(ar.tail, newMap)
        } else {
          val newMap = countMap ++ Map(currentKeyVal -> 1)
          count_filter(ar.tail, newMap)
        }
      }
    }

    val counts_filtered = count_filter(arrayData, Map())


    // STEP 2.

    // Note 2.1
    // Might be better for memory to just accumulate these values...
    //val countsOne = counts_filtered.map(x => (x, 1))
    //countsOne.foreach(x => println(x))
    //countsOne

    // Note 2.2
    // Simple solution
    //val groupedByKey = counts_filtered.groupBy(x => x.get(0))
    //val uniqueKeyVal = groupedByKey.filter(x => x._2.toList.length == 1).values
    //uniqueKeyVal.foreach(x => println(x))

    @tailrec
    def removeDuplicateKey(ar: Iterable[Row], keyRowMap: Map[Int, Iterable[Row]]): Iterable[Row] = {
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
          removeDuplicateKey(ar.tail, newMap)
        } else {
          val newMap = keyRowMap ++ Map(currentKey -> Iterable(currentRow))
          removeDuplicateKey(ar.tail, newMap)
        }
      }
    }

    val uniquelyOddRows = removeDuplicateKey(counts_filtered,  Map())

    // convert Map back into Spark Dataframe
    val rdd = spark.sparkContext.parallelize(uniquelyOddRows.toSeq)
    val uniquelyOdd_df = spark.createDataFrame(rdd, customSchema)
    uniquelyOdd_df
  }

}