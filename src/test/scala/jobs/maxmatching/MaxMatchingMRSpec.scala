package jobs.maxmatching

import constants.{JobsConfigConstants, LogConstants, LogMessages, MaxMatchingJobConstants}
import entity.MatchValueWritableEntity
import jobs.JobsBaseSpec
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{OutputCollector, Reporter}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import utils.{LogReader, ObtainConfigReference}

class MaxMatchingMRSpec extends JobsBaseSpec with MockitoSugar {

  //mock objects
  val output = mock[OutputCollector[Text, MatchValueWritableEntity]]
  val reporter = mock[Reporter]

  // testing classes
  val maxMatchingMapper = new MaxMatchingMapper
  val maxMatchingReducer = new MaxMatchingReducer

  // testing related params
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, MaxMatchingJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in mapper")
  }
  val one = new IntWritable(1)
  val logEntity = LogReader.parseLogStr(LogMessages.LOGWARNMSG)

  behavior of "Maximum match mapper"

  it should "write message matched with log" in {
    // execute map
    maxMatchingMapper.map(new LongWritable, new Text(LogMessages.LOGWARNMSG), output, reporter)

    // verify output
    verify(output).collect(ArgumentMatchers.refEq(new Text(logEntity.messageType)),
      ArgumentMatchers.eq(new MatchValueWritableEntity(new Text(logEntity.timestamp),
        new Text(logEntity.message), new IntWritable(logEntity.message.length))))

  }


}
