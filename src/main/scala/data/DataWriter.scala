package data

import DataHandlers.getListOfFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import purecsv.safe._

object DataWriter {

  def zip[A](path: String, output: List[A]): List[(String, A)] = {
    val listFile = getListOfFiles(path)
    val filePaths = listFile.map(x=>
      x.split("/").dropRight(1).mkString("/")
      + "/outputFolder/"
      + x.split("/").last)
    val zipNameResult = filePaths.zip(output)
    zipNameResult
  }

  def writeDataFrame(output: DataFrame, path: String) = {
    output.write.save(path)
    Unit
  }

  def writeSeq(output: Seq[KeyVal], path: String) = {
    output.writeCSVToFileName(path, header=Some(Seq("key", "value")))
    Unit
  }

  def writeArray(output: Array[KeyVal], path: String) = {
    import purecsv.safe._
    val out = output.toSeq
    out.writeCSVToFileName(path, header=Some(Seq("key", "value")))
    Unit
  }

  def writeRDD(output: RDD[KeyVal], path: String) = {
    import purecsv.safe._
    val out = output.collect().toSeq
    out.writeCSVToFileName(path, header=Some(Seq("key", "value")))
    Unit
  }

}