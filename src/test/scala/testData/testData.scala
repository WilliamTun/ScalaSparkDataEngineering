package testData

import data.dataHandlers.customSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

object testData {
  val spark = SparkSession.builder
    .appName("SparkSessionExample")
    .master("local[4]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()

  val rowData = Seq(
    Row(1, 3),
    Row(1, 3),
    Row(1, 2),
    Row(2, 5),
    Row(2, 6)
  )

  val rowData2 = Seq(
    Row(1, 2),
    Row(2, 5),
    Row(2, 6)
  )

  val rawDF = spark.createDataFrame(
    spark.sparkContext.parallelize(rowData),
    StructType(customSchema)
  )

  val oddDF = spark.createDataFrame(
    spark.sparkContext.parallelize(rowData2),
    StructType(customSchema)
  )


  val oddMap = Map(
    1 -> Map(2 -> 1),
    2 -> Map(5 -> 1, 6 -> 1)
  )


  val rawRdd = spark.sparkContext.parallelize(List((Row(1, 2), 1), (Row(2, 5), 1), (Row(2, 6), 1)))

  val rawIterable = Array(
    Row(1, 3),
    Row(1, 3),
    Row(1, 2),
    Row(2, 5),
    Row(2, 6)
  )

  val oddIterable = Seq(
    Row(1, 2),
    Row(2, 5),
    Row(2, 6)
  )

}
