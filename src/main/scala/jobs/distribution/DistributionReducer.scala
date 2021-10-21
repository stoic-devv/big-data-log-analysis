package jobs.distribution

import constants.LogConstants
import entity.MsgTypeCountWritableEntity
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}

import java.util.Iterator;

class DistributionReducer extends MapReduceBase with Reducer[Text,MsgTypeCountWritableEntity,Text,Text] {

  val SEPARATOR = ","

  /**
   * Processes iterator for message types and updates the relevant counters
   **/
  def reduce(key: Text, values: Iterator[MsgTypeCountWritableEntity],
             output: OutputCollector[Text, Text], reporter: Reporter): Unit = {

    def cumulateMsg(warnCt: Int, infoCt: Int, debugCt: Int, errCt: Int, itr: Iterator[MsgTypeCountWritableEntity]): List[MsgTypeCountWritableEntity] = {
      itr.hasNext match {
        case true => {
          val msgCountWritable = itr.next()
          msgCountWritable.msgType.toString match {
            case LogConstants.WARN => cumulateMsg(warnCt + 1, infoCt, debugCt, errCt, itr)
            case LogConstants.INFO => cumulateMsg(warnCt, infoCt + 1, debugCt, errCt, itr)
            case LogConstants.DEBUG => cumulateMsg(warnCt, infoCt, debugCt + 1, errCt, itr)
            case LogConstants.ERROR => cumulateMsg(warnCt, infoCt, debugCt, errCt + 1, itr)
            case _ => ???
          }
        }
        case false => List(new MsgTypeCountWritableEntity(new Text(LogConstants.WARN), new IntWritable(warnCt)),
          new MsgTypeCountWritableEntity(new Text(LogConstants.INFO), new IntWritable(infoCt)),
          new MsgTypeCountWritableEntity(new Text(LogConstants.DEBUG), new IntWritable(debugCt)),
          new MsgTypeCountWritableEntity(new Text(LogConstants.ERROR), new IntWritable(errCt)))
      }
    }

    val out = cumulateMsg(0,0,0,0, values)
    val sb = new StringBuilder
    out.foreach(t => sb.append(t.msgCount.toString).append(SEPARATOR))
    output.collect(key, new Text(sb.toString().stripSuffix(SEPARATOR)))
  }
}
