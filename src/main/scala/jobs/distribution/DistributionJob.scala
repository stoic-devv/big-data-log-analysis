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

    // set job name and jar
    val distConf = new JobConf(this.getClass)
    distConf.setJobName("Distribution of message types")
    distConf.setJarByClass(this.getClass)

    // set output key-value class
    distConf.setOutputKeyClass(classOf[Text])
    distConf.setOutputValueClass(classOf[Text])
    
    // set mapper and reducer class
    distConf.setMapperClass(classOf[DistributionMapper])
    distConf.setReducerClass(classOf[DistributionReducer])

    // set mapper key-value class
    distConf.setMapOutputKeyClass(classOf[Text])
    distConf.setMapOutputValueClass(classOf[MsgTypeCountWritableEntity])

    // formatting
    distConf.setInputFormat(classOf[TextInputFormat])
    distConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    distConf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])

    //set input adn output paths
    FileInputFormat.setInputPaths(distConf, new Path(inputPath))
    FileOutputFormat.setOutputPath(distConf, 
      new Path(outputPathBase + jobsConfig.getString(JobsConfigConstants.DISTRIBUTION_OUTPUT_DIR)))

    // run job
    JobClient.runJob(distConf)
  }