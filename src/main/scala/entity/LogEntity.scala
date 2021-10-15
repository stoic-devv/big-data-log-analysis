package entity

class LogEntity(val timestamp: String, val messageType: String, val message: String) {
  override def toString: String = {
    return "%s %s %s".format(timestamp, messageType, message)
  }
}