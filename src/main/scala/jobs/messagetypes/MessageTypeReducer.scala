package jobs.messagetypes

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}

import java.util.Iterator

class MessageTypeReducer extends MapReduceBase with Reducer[Text,IntWritable,Text,IntWritable] {

  /**
   * Adds number of messages by message type key
   **/
  def reduce(key: Text, values: Iterator[IntWritable],
             output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {

    def calcSum(accum: Int, itr: Iterator[IntWritable]): Int = {
      itr.hasNext match {
        case true => calcSum(accum + itr.next().get(), itr)
        case false => accum
      }
    }
    output.collect(key, new IntWritable(calcSum(0, values)))
  }
}
