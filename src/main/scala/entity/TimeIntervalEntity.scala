package entity

class TimeIntervalEntity(startTime: String, endTime: String) {
  override def toString: String = {
    return "%s-%s".format(startTime, endTime)
  }
}