package entity

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable}

import java.io.{DataInput, DataOutput, IOException}

/**
 * Intermediate output writable for the jobs.distribution job
 * eg: WARN, 1
 **/
// NOTE: var is used because hadoop sets the param values from a datastream in readFields
class MsgTypeCountWritableEntity(var msgType: Text, var msgCount: IntWritable) extends WritableComparable[MsgTypeCountWritableEntity] {

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

  @throws(classOf[IOException])
  override def compareTo(o: MsgTypeCountWritableEntity): Int = {
    msgCount.compareTo(o.msgCount)
  }

  override def toString: String = {
    return "%s: %s".format(msgType, msgCount)
  }
}