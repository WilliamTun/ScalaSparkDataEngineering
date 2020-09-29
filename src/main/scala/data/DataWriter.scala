package data

import DataReader.getListOfFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import purecsv.safe._

object DataWriter {

  def zip[A](path: String, output: List[A]): List[(String, A)] = {
    val listFile = getListOfFiles(path)
    val filePaths = listFile.map(fpath => {
        val splitPath = fpath.split("/")
        splitPath.dropRight(1).mkString("/") + "/outputFolder/" + splitPath.last
      })
    filePaths.zip(output)
  }

  def writeDataFrame(output: DataFrame, path: String): Unit = {
    output.coalesce(1).write.option("header","true").save(path)
  }

  def writeSeq(output: Seq[KeyVal], path: String): Unit = {
    output.writeCSVToFileName(path, header=Some(Seq("key", "value")))
  }

  def writeArray(output: Array[KeyVal], path: String): Unit = {
    import purecsv.safe._
    val out = output.toSeq
    out.writeCSVToFileName(path, header=Some(Seq("key", "value")))
  }

  def writeRDD(output: RDD[KeyVal], path: String): Unit = {
    output.map{ kv =>
      var line = kv.key.toString + "," + kv.value.toString
      line
    }.saveAsTextFile(path)
  }

}