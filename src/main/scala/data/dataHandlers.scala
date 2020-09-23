package data

import java.io.File
import scala.io.Source
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

object dataHandlers {

  val customSchema = StructType(Array(
    StructField("KEY", IntegerType, true),
    StructField("VALUE", IntegerType, true)
  ))

  def ReadData(spark: SparkSession, path:String, format: String): DataFrame = {

    if (format == "csv") {
      val df = spark.read
        .format("csv")
        .option("header", "true")
        .schema(customSchema)
        .load(path)
        .na.fill(0)
      df
    } else {
      val df = spark.sqlContext.read
        .format("csv")
        .option("header", "true")
        .option("delimiter", "\t")
        .schema(customSchema)
        .load(path)
        .na.fill(0)
      df
    }

  }


  def getAWSCredentials(path: String): Map[String, String] = {
    val creds = Source.fromFile(path).getLines
    val credArray = creds.map(x => x.split("="))
    val credMap = credArray.foldLeft(Map.empty[String, String]) { case (map, elem) =>
      map + (elem(0).trim() -> elem(1).trim())
    }
    credMap
  }

  def WriteData(spark: SparkSession, df: DataFrame, path: String): Unit = {

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"),
      ("header", "true"))

    df.coalesce(1)
      .write
      .options(tsvWithHeaderOptions)
      .csv(path)  // eg. "s3n://bucket/folder/parquet/myFile"

    Unit
  }


  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith("sv"))
      .map(_.getPath).toList
  }


  def ReadAllFiles[A](typeInput: A, spark: SparkSession, path: String): List[A] = {
    val files = getListOfFiles(path)

    typeInput match {
      case typeIn: Seq[Row] =>
        val listRawData = files.map (inputPath => {
        val format = inputPath.takeRight (3)
          val data = ReadData (spark, inputPath, format)
        val d = data.collect().toSeq
        d
      })
        listRawData.asInstanceOf[List[A]]

      case typeIn: Array[Row] =>
        val listRawData = files.map (inputPath => {
          val format = inputPath.takeRight (3)
          val data = ReadData (spark, inputPath, format)
          val d = data.collect() // array[Row]
          d
        })
        listRawData.asInstanceOf[List[A]]

      case typeIn: RDD[Row] =>
        val listRawData = files.map (inputPath => {
          val format = inputPath.takeRight (3)
          val data = ReadData (spark, inputPath, format)
          val d = data.rdd
          d
        })
        listRawData.asInstanceOf[List[A]]



      case typeIn: DataFrame =>
        val listRawData = files.map (inputPath => {
          val format = inputPath.takeRight (3)
          val data = ReadData (spark, inputPath, format)
          data
        })
        listRawData.asInstanceOf[List[A]]


      case _ =>  throw new Exception("Files requested to be read in as an unsupported Datatype");

    }
  }
}
