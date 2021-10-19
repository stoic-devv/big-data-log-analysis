package jobs.maxmatching

import constants.{JobsConfigConstants, LogConstants, MaxMatchingJobConstants}
import entity.MatchValueWritableEntity
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import utils.LogReader.parseLogStr
import utils.{CreateLogger, ObtainConfigReference}

class MaxMatchingMapper extends MapReduceBase with Mapper[Object, Text, Text, MatchValueWritableEntity]{

  val logger = CreateLogger(classOf[MaxMatchingMapper])
  val one = new IntWritable(1)
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, MaxMatchingJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in mapper")
  }
  val pattern = config.getString(MaxMatchingJobConstants.PATTERN).r

  override def map(key: Object, value: Text, output: OutputCollector[Text, MatchValueWritableEntity], reporter: Reporter): Unit = {
    val logEntity = parseLogStr(value.toString)
    
    pattern.findFirstMatchIn(logEntity.message) match {
      case Some(_) => output.collect(new Text(logEntity.messageType), 
        new MatchValueWritableEntity(new Text(logEntity.timestamp), 
          new Text(logEntity.message), new IntWritable(logEntity.message.length)))
      case None => {}
    }
  }

}

