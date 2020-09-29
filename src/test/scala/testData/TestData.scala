package testData

import org.apache.spark.sql.SparkSession
import data.KeyVal

object TestData {
  val spark = SparkSession.builder
    .appName("SparkSessionTest")
    .master("local[4]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()

  val rowData = Seq(
    KeyVal(key = 1, value = 3),
    KeyVal(key = 1, value = 3),
    KeyVal(key = 1, value = 2),
    KeyVal(key = 2, value = 5),
    KeyVal(key = 2, value = 6)
  )

  val rowData2 = Seq(
    KeyVal(key = 1, value = 2),
    KeyVal(key = 2, value = 5),
    KeyVal(key = 2, value = 6)
  )

  import spark.implicits._
  val rawDF = rowData.toDF()
  val oddDF = rowData2.toDF()

  val oddMap = Map(
    1 -> Map(2 -> 1),
    2 -> Map(5 -> 1, 6 -> 1)
  )

  val oddRDD = spark.sparkContext.parallelize(List((KeyVal(1, 2), 1), (KeyVal(2, 5), 1), (KeyVal(2, 6), 1)))

  val rawIterable = Array(
    KeyVal(1, 3),
    KeyVal(1, 3),
    KeyVal(1, 2),
    KeyVal(2, 5),
    KeyVal(2, 6)
  )

  val oddIterable = Seq(
    KeyVal(1, 2),
    KeyVal(2, 5),
    KeyVal(2, 6)
  )

  val rawSeqRow = Seq(
    KeyVal(1, 3),
    KeyVal(1, 3),
    KeyVal(1, 2),
    KeyVal(2, 5),
    KeyVal(2, 6)
  )

  val rawRDD = spark.sparkContext.parallelize(List(
    KeyVal(1, 3),
    KeyVal(1, 3),
    KeyVal(1, 2),
    KeyVal(2, 5),
    KeyVal(2, 6)
  ))

  val rawArray = Array(
    KeyVal(1, 3),
    KeyVal(1, 3),
    KeyVal(1, 2),
    KeyVal(2, 5),
    KeyVal(2, 6)
  )

}
