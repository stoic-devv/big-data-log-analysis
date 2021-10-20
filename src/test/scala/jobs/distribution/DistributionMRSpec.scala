package jobs.distribution

import constants.{DistributionJobConstants, JobsConfigConstants, LogMessages}
import entity.MsgTypeCountWritableEntity
import jobs.JobsBaseSpec
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{OutputCollector, Reporter}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import utils.DistributionUtils.getResetWindow
import utils.{DistributionUtils, LogReader, ObtainConfigReference}

import java.util


class DistributionMRSpec extends JobsBaseSpec with MockitoSugar {

  // mock objects
  val mapOutput = mock[OutputCollector[Text, MsgTypeCountWritableEntity]]
  val reduceOutput = mock[OutputCollector[Text, Text]]
  val reporter = mock[Reporter]

  // test classes
  val distMapper = new DistributionMapper
  val distReducer = new DistributionReducer

  // params used in testing
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, DistributionJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in test spec")
  }
  val interval = config.getInt(DistributionJobConstants.INTERVAL)
  val resetInterval = getResetWindow(interval)
  val logEntity = LogReader.parseLogStr(LogMessages.LOGINFOMSG)
  val msgCountEntity = new MsgTypeCountWritableEntity(new Text(logEntity.messageType), new IntWritable(1))
  val tmInterval = DistributionUtils.getTimeInterval(logEntity.timestamp, interval,resetInterval)


  behavior of "Distribution Mapper"

  it should "map the log message timestamp to an interval key and custome msg count writable" in {

    // call map
    distMapper.map(new LongWritable(), new Text(LogMessages.LOGINFOMSG), mapOutput, reporter)

    //verify output
    verify(mapOutput).collect(ArgumentMatchers.refEq(new Text(tmInterval.toString)),
      ArgumentMatchers.refEq(msgCountEntity))
  }

  behavior of "Distribution Reducer"

  it should "count the number of log messages by type" in {
    val msgWritables = new util.ArrayList[MsgTypeCountWritableEntity]()
    msgWritables.add(msgCountEntity)
    val reduceKey = new Text(tmInterval.toString)
    val expectedOutput = "0,1,0,0"

    // call reduce
    distReducer.reduce(reduceKey, msgWritables.iterator(), reduceOutput, reporter)

    // verify output
    verify(reduceOutput).collect(ArgumentMatchers.refEq(reduceKey), 
      ArgumentMatchers.refEq(new Text(expectedOutput)))

  }
}
