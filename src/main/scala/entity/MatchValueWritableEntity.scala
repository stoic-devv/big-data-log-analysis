package entity

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable}

import java.io.{DataInput, DataOutput, IOException}

// NOTE: var is used because hadoop sets the param values from a datastream in readFields
class MatchValueWritableEntity(var msgTime: Text, var msg: Text, var length: IntWritable) extends WritableComparable[MatchValueWritableEntity] {
  def this() = {
    this(new Text(), new Text(), new IntWritable(-1))
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit = {
    msgTime.readFields(in)
    msg.readFields(in)
    length.readFields(in)
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput): Unit = {
    msgTime.write(out)
    msg.write(out)
    length.write(out)
  }

  @throws(classOf[IOException])
  override def compareTo(o: MatchValueWritableEntity): Int = {
    val primaryCompare = length.compareTo(o.length)
    primaryCompare match {
      case 0 => msgTime.compareTo(o.msgTime)
      case _ => primaryCompare
    }
  }

  override def toString: String = {
    return "%s, %s, %s".format(msgTime, msg, length)
  }
}
