package entity

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable}

import java.io.{DataInput, DataOutput, IOException}

/**
 * Composite key is hadoop writable used for secondary sort
 * For eg: to sort by value in (key, value) use < key, value > as a composite key
 **/
// NOTE: var is used because hadoop sets the param values from a datastream in readFields
class CompositeKeyWritableEntity(var keyOne: Text, var keyTwo: IntWritable) extends WritableComparable[CompositeKeyWritableEntity] {

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

  /**
   * for sorting in descending order of values in < key, value> composite object
   **/
  @throws(classOf[IOException])
  override def compareTo(o: CompositeKeyWritableEntity): Int = {
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