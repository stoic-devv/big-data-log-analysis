package jobs.messagetypes

import constants.JobsConfigConstants
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import utils.ObtainConfigReference

/**
 * Sets config and runs the job for getting count by log message type
 **/
class MessageTypeJob

object MessageTypeJob:

  def apply(inputPath: Path): Unit = {

    val jobsConfig = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    val msgTypeConf = new JobConf(this.getClass)
    msgTypeConf.setJobName("Count logs by message types")
    msgTypeConf.setJarByClass(this.getClass)

    msgTypeConf.setOutputKeyClass(classOf[Text])
    msgTypeConf.setOutputValueClass(classOf[IntWritable])

    // setting mapper
    msgTypeConf.setMapperClass(classOf[MessageTypeMapper])

    // setting reducer
    msgTypeConf.setReducerClass(classOf[MessageTypeReducer])

    // set reducer as combiner for aggregation
    msgTypeConf.setCombinerClass(classOf[MessageTypeReducer])

    msgTypeConf.setInputFormat(classOf[TextInputFormat])
    msgTypeConf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(msgTypeConf, inputPath)
    FileOutputFormat.setOutputPath(msgTypeConf, new Path(jobsConfig.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
      jobsConfig.getString(JobsConfigConstants.MESSAGE_TYPE_OUTPUT_DIR)))

    JobClient.runJob(msgTypeConf)
  }
