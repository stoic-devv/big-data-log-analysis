package distribution

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.MsgTypeCountWritableEntity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat, TextOutputFormat}
import utils.DistributionUtils.*
import utils.{DistributionUtils, ObtainConfigReference}

class DistributionJob

object DistributionJob:

  def apply(conf: JobConf): Unit = {

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])

    conf.setMapperClass(classOf[DistributionMapper])
    conf.setReducerClass(classOf[DistributionReducer])

    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[MsgTypeCountWritableEntity])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
  }