package jobs.distribution

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.MsgTypeCountWritableEntity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import utils.DistributionUtils.*
import utils.{DistributionUtils, ObtainConfigReference}

/**
 * Sets config and runs the distribution job
 **/
class DistributionJob

object DistributionJob:

  def apply(inputPath: String, outputPathBase: String): Unit = {

    val jobsConfig = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    val distConf = new JobConf(this.getClass)
    distConf.setJobName("Distribution of message types")
    distConf.setJarByClass(this.getClass)

    distConf.setOutputKeyClass(classOf[Text])
    distConf.setOutputValueClass(classOf[Text])
    
    distConf.setMapperClass(classOf[DistributionMapper])
    distConf.setReducerClass(classOf[DistributionReducer])

    distConf.setMapOutputKeyClass(classOf[Text])
    distConf.setMapOutputValueClass(classOf[MsgTypeCountWritableEntity])

    distConf.setInputFormat(classOf[TextInputFormat])
    distConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    distConf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.setInputPaths(distConf, new Path(inputPath))
    FileOutputFormat.setOutputPath(distConf, 
      new Path(outputPathBase + jobsConfig.getString(JobsConfigConstants.DISTRIBUTION_OUTPUT_DIR)))

    JobClient.runJob(distConf)
  }