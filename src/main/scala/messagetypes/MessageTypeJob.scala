package messagetypes

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat, TextOutputFormat}

class MessageTypeJob

object MessageTypeJob:

  def apply(conf: JobConf): Unit = {

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    // setting mapper
    conf.setMapperClass(classOf[MessageTypeMapper])

    // setting reducer
    conf.setReducerClass(classOf[MessageTypeReducer])

    // set reducer as combiner for aggregation
    conf.setCombinerClass(classOf[MessageTypeReducer])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
  }
