package entity

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable}

import java.io.{DataInput, DataOutput, IOException}

/**
 * Intermediate output writable for the distribution job
 * eg: WARN, 1
 **/
class MsgTypeCountWritableEntity(val msgType: Text, val msgCount: IntWritable) extends WritableComparable[MsgTypeCountWritableEntity] {

  def this() = {
    this(new Text(), new IntWritable())
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit = {
    msgType.readFields(in)
    msgCount.readFields(in)
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput): Unit = {
    msgType.write(out)
    msgCount.write(out)
  }

  /**
  * compares the count param
  * */
  @throws(classOf[IOException])
  override def compareTo(o: MsgTypeCountWritableEntity): Int = {
    msgCount.compareTo(o.msgCount)
  }

  override def toString: String = {
    return "%s: %s".format(msgType, msgCount)
  }
}