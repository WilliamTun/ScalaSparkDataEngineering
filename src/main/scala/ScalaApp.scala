import org.apache.spark.sql.SparkSession
import data.DataHandlers.{readAllFiles}
import logic.Solution.{solve, write}

object ScalaApp {
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

      //// Run Locally:
      //if (args.length == 0) {
      //  throw new Exception("Parameter for path to resources folder required")
      //}
      //val resourcePath = args(0)

      val spark = SparkSession.builder
        .appName("SparkSessionExample")
        .master("local[4]")
        .config("spark.sql.warehouse.dir", "target/spark-warehouse").getOrCreate()

      val resourcePath = "/Users/williamtun/Documents/Code/Job_Assessments/convex2/ScalaSparkDataEngineering/src/main/resources/"
      val allFiles = readAllFiles(resourcePath)

      val choice = 1

      if (choice == 1) {
        import spark.implicits._
        //val allRDDs = allFiles.map( seqKeyVal => seqKeyVal.toDF())
        //val oneDataSet = allRDDs(0)
        import logic.solutionStyle.Solution1Spark
        val sol = new Solution1Spark()
        val allDF = allFiles.map( seqKeyVal => seqKeyVal.toDF())
        val output = solve(allDF)
        write(output, resourcePath)
      }
      else if (choice == 2) {
        import spark.implicits._
        val allDF = allFiles.map( seqKeyVal => seqKeyVal.toDF())
        val output = solve(allDF)
        write(output, resourcePath)
      } else if (choice == 3 ) {
        val allSeq = allFiles
        val output = solve(allSeq)
        write(output, resourcePath)
      } else if (choice == 4) {
        val sc = spark.sparkContext
        val allRDDs = allFiles.map( seqKeyVal => sc.parallelize(seqKeyVal))
        val output = solve(allRDDs)
        write(output, resourcePath)
      } else {
        val allArray = allFiles.map( seqKeyVal => seqKeyVal.toArray)
        val output = solve(allArray)
        write(output, resourcePath)
      }

    }
}

// "WRITE ALL METHOD" + Try catch to handle EMPTY files read in / inappropriate files read in...
// 1.1   remove ds store:
//       https://stackoverflow.com/questions/107701/how-can-i-remove-ds-store-files-from-a-git-repository
