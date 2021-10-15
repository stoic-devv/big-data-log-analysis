package utils

import entity.LogEntity

import java.util
import java.util.Iterator
import scala.io.Source

class LogReader

object LogReader:

  // obtain log config
  private val logConf = ObtainConfigReference("log-params", "log-params") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate log config file")
  }


  /**
   * Parses a given log for relevant values
   * @param line: input line
   * @return : List of timestamp, message type and message
   * */
  def parseLogStr(line: String): LogEntity =

    val words = line.split(" ")

    // output indices of line depends on the message type

    // INFO and WARN
    if (words(logConf.getInt("logtypeidx")).equals(logConf.getString("info")) ||
      words(logConf.getInt("logtypeidx")).equals(logConf.getString("warn"))) {
      return LogEntity(words(logConf.getInt("timeidx")),
        words(logConf.getInt("logtypeidx")), words(logConf.getInt("infowarnidx")))
    }

    // ERROR and DEBUG
    else {
      return LogEntity(words(logConf.getInt("timeidx")),
        words(logConf.getInt("logtypeidx")), words(logConf.getInt("errordebugidx")))
    }

  /*def readFile(fname: String): Unit =
    for (line <- Source.fromFile(fname).getLines) {
      println(parseLogStr(line))
    }*/

  /*def main(args: Array[String]): Unit =

    val u = new util.ArrayList[Int]()
    u.add(1)
    u.add(2)
    u.add(3)

    def calcSum(accum: Int, itr: Iterator[Int]): Int = {
      itr.hasNext match {
        case true => calcSum(accum + itr.next(), itr)
        case false => accum
      }
    }

    println(calcSum(0, u.iterator()))


    //readFile("C:\\Users\\achand50\\Downloads\\cs441\\big-data-log-analysis\\src\\main\\scala\\utils\\sample.log")
*/
