import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import scala.io.Source

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

    df.coalesce(1) // Writes to a single file
      .write
      .options(tsvWithHeaderOptions)
      .csv(path)  // eg. "s3n://bucket/folder/parquet/myFile"

    Unit
  }


}
