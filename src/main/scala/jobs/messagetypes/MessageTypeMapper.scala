package jobs.messagetypes

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import utils.CreateLogger
import utils.LogReader.parseLogStr

class MessageTypeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {

  val logger = CreateLogger(classOf[MessageTypeMapper])
  
  /**
   * Maps line to message type and count 1
   **/
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
    val logEntity = parseLogStr(value.toString)
    output.collect(new Text(logEntity.messageType), new IntWritable(1))
  }
}
