package data

import java.io.File
import scala.io.Source

object DataHandlers {

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
    raw.filter(x => x != "").map(x => x.split("[,\t]")).map(xx =>
      if (xx.length == 2) {
        KeyVal(xx(0).toInt, xx(1).toInt)
      } else if (xx.length == 1) {
        KeyVal(xx(0).toInt, 0)
      } else {
        null
      }
    ).toSeq
  }

  def readAllFiles(resourcePath: String): List[Seq[KeyVal]] = {
    val listFile = getListOfFiles(resourcePath)
    listFile.map(x => readFile(x))
  }

  def getAWSCredentials(path: String): Map[String, String] = {
    val creds = Source.fromFile(path).getLines
    val credArray = creds.map(x => x.split("="))
    val credMap = credArray.foldLeft(Map.empty[String, String]) { case (map, elem) =>
      map + (elem(0).trim() -> elem(1).trim())
    }
    credMap
  }

}
