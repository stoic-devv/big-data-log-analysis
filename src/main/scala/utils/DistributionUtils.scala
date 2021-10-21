package utils

import entity.TimeIntervalEntity
import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormat

class DistributionUtils

object DistributionUtils:

  def getResetWindow(dur: Int): Int = {
    dur match {
      case v if v < 1000 => 1000
      case v if v < 60*1000 => 60*1000
      case v if v < 60*60*1000 => 60*60*1000
      case v if v < 24*60*60*1000 => 24*60*60*1000
      case _ => throw new RuntimeException("Invalid interval argument. Please put a value (in milliseconds) between " + 0 + " and " + 24*60*60*1000)
    }
  }

  /**
   * Returns the time interval in which the given interval lies
   **/
  def getTimeInterval(tmStr: String, gi: Int, rw: Int): TimeIntervalEntity = {
    
    // timestamp format in log file
    val utf = DateTimeFormat.forPattern("HH:mm:ss.SSS")
    
    // convert to LocalTime object
    val tmStamp = utf.parseLocalTime(tmStr)
    val tmInMillis = tmStamp.getMillisOfDay
    
    // interval number within a range
    val bucketNum = (tmInMillis%rw)/gi
    
    // interval start
    val tmStart = tmInMillis - tmInMillis%rw
    // interval bucket start
    val tmLeft = tmStart + bucketNum*gi
    // interval bucket end
    val tmRight = tmLeft + gi
    
    return TimeIntervalEntity(LocalTime.fromMillisOfDay(tmLeft).toString, LocalTime.fromMillisOfDay(tmRight).toString)
  }
