package jobs.maxmatching

import entity.MatchValueWritableEntity
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, OutputCollector, Reducer, Reporter}

import java.util.Iterator

class MaxMatchingReducer extends MapReduceBase with Reducer[Text,MatchValueWritableEntity,Text,MatchValueWritableEntity] {

  /**
   * Calculates the maximum value for a given key
   **/
  def reduce(key: Text, values: Iterator[MatchValueWritableEntity],
             output: OutputCollector[Text, MatchValueWritableEntity], reporter: Reporter): Unit = {

    def calcMaxLength(maxEntity: MatchValueWritableEntity, itr: Iterator[MatchValueWritableEntity]): MatchValueWritableEntity = {
      itr.hasNext match {
        case true => {
          val currEntity = itr.next()
          maxEntity.length.compareTo(currEntity.length) match {
            case -1 | 0 => calcMaxLength(currEntity, itr)
            case _ => calcMaxLength(maxEntity, itr)
          }
        }
        case false => maxEntity
      }
    }
    output.collect(key, calcMaxLength(new MatchValueWritableEntity(), values))
  }
}
