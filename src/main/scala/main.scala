import org.apache.spark.sql.SparkSession
import dataHandlers.{ReadData, WriteData, getAWSCredentials}
import transformFunctions.{solution1, solution2, solution3, solution4, solution5}

object main {
  def main(args: Array[String]): Unit = {

    //if (args.length != 3) {
    //  println("Please run: sbt run <input> <output> <credentials>")
    //  println("For example: sbt run output2.tsv output4.tsv credentials")

    //} else {

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

      // Read in Data LOCALLY
      val inputPath = "/Users/williamtun/Documents/Code/Job_Assessments/convex/src/main/resources/data.csv"

      val format = inputPath.takeRight(3)
      val rawDF = ReadData(spark, inputPath, format)


      // hard coded parameter to select solution method.
      // solution 1 = spark context
      // solution 2 = sql context
      // solution 3 = standard scala
      // solution 4 = rdd approach
      // solution 5 = recursive approach
      val solution = 5

      val outputDF = if (solution == 1) {
        solution1(rawDF, spark)
      } else if (solution == 2) {
        solution2(rawDF, spark)
      } else if (solution == 3) {
        solution3(rawDF, spark)
      } else if (solution == 4) {
        solution4(rawDF, spark)
      } else {
          solution5(rawDF, spark)
      }

      outputDF.show

      // Write out Data
      //WriteData(spark, outputDF, outputPath)

    }

  //}
}


// More things that can be done:
// -1. set write so that overwrite can occur.
// -2. add try catch error handling - eg. when csv / tsv does not tail the file name.
// -3. could add unit tests eg. tests to handle wrong parameter input
