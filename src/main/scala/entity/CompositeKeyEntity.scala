package entity

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable}

import java.io.{DataInput, DataOutput, IOException}

class CompositeKeyEntity(var keyOne: Text, var keyTwo: IntWritable) extends WritableComparable[CompositeKeyEntity] {

  def this() = {
    this(new Text(), new IntWritable())
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit = {
    keyOne.readFields(in)
    keyTwo.readFields(in)
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput): Unit = {
    keyOne.write(out)
    keyTwo.write(out)
  }

  @throws(classOf[IOException])
  override def compareTo(o: CompositeKeyEntity): Int = {
    val primaryCompare = -1*keyTwo.compareTo(o.keyTwo)
    primaryCompare match {
      case 0 => keyOne.compareTo(o.keyOne)
      case _ => primaryCompare
    }
  }

  override def toString: String = {
    return "%s: %s".format(keyOne, keyTwo)
  }
}