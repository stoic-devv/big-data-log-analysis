package utils

import entity.{LogEntity}


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

