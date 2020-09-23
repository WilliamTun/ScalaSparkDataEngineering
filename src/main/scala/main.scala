import org.apache.spark.sql.{Row, SparkSession}
import data.dataHandlers.{WriteData, getAWSCredentials, getListOfFiles}
import data.dataHandlers.{ReadAllFiles, customSchema}
import logic.solution.solve

object main extends App {
    override def main(args: Array[String]): Unit = {

      /**
      // Note. please change base path from local to AWS path when working with amazon.
      //       eg.  "s3n://bucket/folder/parquet/myFile"
      val basePath = "PATH/TO/S3"
      val inputPath = basePath + args(0) // "data.tsv"
      val outputPath = basePath + args(1) //"output.tsv"
      // please change string accordingly to where S3 credential file is held.
      val awsCredentialPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/" + args(2) // credentials

      val spark = SparkSession.builder
      .appName("SparkSessionExample")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()

      // get AWS credentials from credentials file
      val credMap = getAWSCredentials(awsCredentialPath)
      // set AWS credentials
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", credMap("aws_access_key_id"))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", credMap("aws_secret_access_key"))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      */

      // To Run Locally, unblock this section, Block above lines and change top level condition
      val spark = SparkSession.builder
        .appName("SparkSessionExample")
        .master("local[4]")
        .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()
      val sc = spark.sparkContext

      val choice = Tuple4(Seq(Row.empty),
                          Array(Row.empty),
                          spark.sparkContext.parallelize(Seq(Row.empty)),
                          spark.emptyDataFrame)

      val filesPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/"
      val listRawData = ReadAllFiles(typeInput = choice._2, spark = spark, path = filesPath)
      val out = solve(listRawData).getOrElse(throw new Exception("could not apply logic to input data"))

      // out.foreach(z => z.collect().foreach(x => println(x)))  // if rdd or dataframe
      // out.map( z => z.foreach(x => println(x))) // if Array[Row] or Seq[Row]

      val listFile = getListOfFiles("/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/")
      val fileNames = listFile.map(x=> x.split("/").last)
      val zipNameResult = fileNames.zip(out)

      zipNameResult.foreach(nameRes => {
          val rd = sc.parallelize(nameRes._2)
          val outputDF = spark.createDataFrame(rd, customSchema)
          val outputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/outputFolder/" + nameRes._1
          WriteData(spark, outputDF, outputPath)
        }
      )
    }
}