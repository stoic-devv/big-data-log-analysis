package jobs.maxmatching

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.MatchValueWritableEntity
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import utils.ObtainConfigReference

/**
 * Sets config and runs the job for finding the maximum length of the log message by type that matches the search query 
 **/
class MaxMatchingJob

object MaxMatchingJob:

  def apply(inputPath: String, outputBasePath: String): Unit = {
    val jobsConfig = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

      val maxMatchConf = new JobConf(this.getClass)
      maxMatchConf.setJobName("Maximum length string by type")
      maxMatchConf.setJarByClass(this.getClass)

    maxMatchConf.setOutputKeyClass(classOf[Text])
    maxMatchConf.setOutputValueClass(classOf[MatchValueWritableEntity])

    maxMatchConf.setMapperClass(classOf[MaxMatchingMapper])
    maxMatchConf.setCombinerClass(classOf[MaxMatchingReducer])
    maxMatchConf.setReducerClass(classOf[MaxMatchingReducer])

    maxMatchConf.setMapOutputKeyClass(classOf[Text])
    maxMatchConf.setMapOutputValueClass(classOf[MatchValueWritableEntity])

    maxMatchConf.setInputFormat((classOf[TextInputFormat]))
    maxMatchConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    maxMatchConf.setOutputFormat(classOf[TextOutputFormat[Text, MatchValueWritableEntity]])

    // set input and output dirs
    FileInputFormat.setInputPaths(maxMatchConf, new Path(inputPath))
    FileOutputFormat.setOutputPath(maxMatchConf, new Path(outputBasePath +
      jobsConfig.getString(JobsConfigConstants.REGEX_OUTPUT_DIR)))

    // run job
    JobClient.runJob(maxMatchConf)

  }
