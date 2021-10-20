package jobs.errdistsort

import constants.{ErrDistSortJobConstants, JobsConfigConstants, LogMessages}
import jobs.JobsBaseSpec
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{OutputCollector, Reporter}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import utils.DistributionUtils.getResetWindow
import utils.{DistributionUtils, LogReader, ObtainConfigReference}

import java.util

class ErrDistSortMRSpec extends JobsBaseSpec with MockitoSugar {

  //mock objects
  val output = mock[OutputCollector[Text, IntWritable]]
  val reporter = mock[Reporter]

  // testing classes
  val errDistMapper = new ErrDistMapper
  val errDistReducer = new ErrDistReducer
  val errSortMapper = new ErrSortMapper

  // testing related params
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, ErrDistSortJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in test")
  }
  val interval = config.getInt(ErrDistSortJobConstants.INTERVAL)
  val resetInterval = getResetWindow(interval)
  val logEntity = LogReader.parseLogStr(LogMessages.LOGINFOMSG)
  val tmInterval = DistributionUtils.getTimeInterval(logEntity.timestamp, interval,resetInterval)

  behavior of "Error log messages distribution mapper"

  it should "filter error logs and assign an interval to the timestamp of the log" in {

    // call map
    errDistMapper.map(new LongWritable(), new Text(LogMessages.LOGERRORMSG), output, reporter)

    // verify output
    verify(output).collect(ArgumentMatchers.refEq(new Text(tmInterval.toString)),
      ArgumentMatchers.refEq(new IntWritable(1)))
  }

  behavior of "Error log messages distribution reducer"

  it should "count the number of log messsages processed by the mappers" in {

    val one = new IntWritable(1)
    val errMsgs = new util.ArrayList[IntWritable]()
    errMsgs.add(one)
    errMsgs.add(one)

    // call reduce
    errDistReducer.reduce(new Text(tmInterval.toString), errMsgs.iterator(), output, reporter)

    // verify output
    verify(output).collect(ArgumentMatchers.refEq(new Text(tmInterval.toString)),
      ArgumentMatchers.refEq(new IntWritable(2)))
  }
}
