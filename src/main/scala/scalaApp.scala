import org.apache.spark.sql.{Row, SparkSession}
import data.dataHandlers.{WriteData, getAWSCredentials, getListOfFiles}
import data.dataHandlers.{ReadAllFiles, customSchema}
import logic.solution.solve

object scalaApp {
    def main(args: Array[String]): Unit = {

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


      // Run Locally:
      if (args.length == 0) {
        throw new Exception("Parameter for path to resources folder required")
      }
      val resourcePath = args(0)

      val spark = SparkSession.builder
        .appName("SparkSessionExample")
        .master("local[4]")
        .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()
      val sc = spark.sparkContext

      val choice = Tuple4(Seq(Row.empty),
                          Array(Row.empty),
                          spark.sparkContext.parallelize(Seq(Row.empty)),
                          spark.emptyDataFrame)

      val listRawData = ReadAllFiles(typeInput = choice._2, spark = spark, path = resourcePath)
      val out = solve(listRawData).getOrElse(throw new Exception("could not apply logic to input data"))



      val listFile = getListOfFiles(resourcePath)
      val fileNames = listFile.map(x=> x.split("/").last)
      val zipNameResult = fileNames.zip(out)

      zipNameResult.foreach(nameRes => {
          val rd = sc.parallelize(nameRes._2)
          val outputDF = spark.createDataFrame(rd, customSchema)
          val outputPath = resourcePath + "outputFolder/" + nameRes._1
          WriteData(spark, outputDF, outputPath)
        }
      )
    }
}

// "WRITE ALL METHOD" + Try catch to handle EMPTY files read in / inappropriate files read in... 


// In order to directly print the results onto the command line, add the following lines of code:
//out.foreach(z => z.collect().foreach(x => println(x)))  // if rdd or dataframe
//out.map( z => z.foreach(x => println(x))) // if Array[Row] or Seq[Row]