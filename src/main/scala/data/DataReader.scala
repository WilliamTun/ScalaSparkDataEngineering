package data

import java.io.File
import scala.io.Source

object DataReader {

  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith("sv"))
      .map(_.getPath).toList
  }

  private def open(path: String) = new File(path)

  private implicit class RichFile(file: File) {
    def read() = Source.fromFile(file).getLines
  }

  def readFile(filePath: String): Seq[KeyVal] = {
    val raw = open(filePath).read.drop(1)
    raw.filter(_ != "").map(_.split("[,\t]")).map(row =>
      if (row.length == 2) {
        KeyVal(row(0).toInt, row(1).toInt)
      } else if (row.length == 1) {
        KeyVal(row(0).toInt, 0)
      } else {
        null
      }
    ).toSeq
  }

  def readAllFiles(resourcePath: String): List[Seq[KeyVal]] = {
    val listFile = getListOfFiles(resourcePath)
    listFile.map(readFile(_))
  }

  /**
  def getAWSCredentials(path: String): Map[String, String] = {
    val creds = Source.fromFile(path).getLines
    val credArray = creds.map(_.split("="))
    val credMap = credArray.foldLeft(Map.empty[String, String]) { case (map, elem) =>
      map + (elem(0).trim() -> elem(1).trim())
    }
    credMap
  }**/

}
