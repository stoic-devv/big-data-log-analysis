package jobs.errdistsort

import constants.{ErrDistSortJobConstants, JobsConfigConstants, LogConstants}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import utils.DistributionUtils.getResetWindow
import utils.LogReader.parseLogStr
import utils.{CreateLogger, DistributionUtils, ObtainConfigReference}

class ErrDistMapper extends MapReduceBase with Mapper[Object, Text, Text, IntWritable]{

  val logger = CreateLogger(classOf[ErrDistMapper])
  val one = new IntWritable(1)
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, ErrDistSortJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in mapper")
  }
  val interval = config.getInt(ErrDistSortJobConstants.INTERVAL)
  val resetInterval = getResetWindow(interval)

  /**
   * Maps timestamp to time interval. Writes to output only if message type is ERROR
   **/
  override def map(key: Object, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
    val logEntity = parseLogStr(value.toString)
    val tmInterval = DistributionUtils.getTimeInterval(logEntity.timestamp, interval, resetInterval)
    logEntity.messageType match {
      case LogConstants.ERROR => output.collect(new Text(tmInterval.toString), one)
      case _ => { }
    }
  }

}
