package jobs.messagetypes

import constants.LogMessages
import jobs.JobsBaseSpec
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{OutputCollector, Reporter}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import utils.LogReader

import java.util

class MessageTypeMRSpec extends JobsBaseSpec with MockitoSugar {

  //mock objects
  val output = mock[OutputCollector[Text, IntWritable]]
  val reporter = mock[Reporter]

  // testing classes
  val messageTypeMapper = new MessageTypeMapper
  val messageTypeReducer = new MessageTypeReducer

  // testing related params
  val logEntity = LogReader.parseLogStr(LogMessages.LOGINFOMSG)
  val one = new IntWritable(1)

  behavior of "Message count mapper"

  it should "write to output message type and count=1" in {

    // execute map
    messageTypeMapper.map(new LongWritable(), new Text(LogMessages.LOGINFOMSG), output, reporter)

    //verify output
    verify(output).collect(ArgumentMatchers.refEq(new Text(logEntity.messageType)), ArgumentMatchers.refEq(one))
  }

  it should "count the number of messages sent to it" in {
    val msgArr = new util.ArrayList[IntWritable]()
    msgArr.add(one)
    msgArr.add(one)

    // execute reduce
    messageTypeReducer.reduce(new Text(logEntity.messageType), msgArr.iterator(), output, reporter)

    // verify output
    verify(output).collect(ArgumentMatchers.refEq(new Text(logEntity.messageType)),
      ArgumentMatchers.eq(new IntWritable(2)))
  }

}
