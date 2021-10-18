import constants.JobsConfigConstants
import distribution.DistributionJob
import messagetypes.MessageTypeJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ToolRunner
import somepackage.SomeJob
import utils.DistributionUtils.getResetWindow
import utils.ObtainConfigReference

object RunJobs:
  def main(args: Array[String]): Unit = {

    val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    args.length match {
      case 2 | 3 => {}
      case _ => throw new RuntimeException("Invalid number of arguments")
    }

    args(0) match {

      // ToDo: remove case -1: kept for testing
      case "-1" => {
        val someConf = new JobConf(MessageTypeJob.getClass)
        someConf.setJobName("some job")
        someConf.setJarByClass(this.getClass)

        // set input and output dirs
        FileInputFormat.setInputPaths(someConf, new Path(args(1)))
        FileOutputFormat.setOutputPath(someConf, new Path(config.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
          "/some_output"))

        // set job configuration
        SomeJob(someConf)

        // run job
        JobClient.runJob(someConf)
      }

      // distributions of the logs
      case "0" => {
        val distJobConf = new JobConf(MessageTypeJob.getClass)
        distJobConf.setJobName("Distribution of messages by types")
        distJobConf.setJarByClass(this.getClass)

        // set input and output dirs
        FileInputFormat.setInputPaths(distJobConf, new Path(args(1)))
        FileOutputFormat.setOutputPath(distJobConf, new Path(config.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
          config.getString(JobsConfigConstants.DISTRIBUTION_OUTPUT_DIR)))

        // set job configuration
        DistributionJob(distJobConf)

        // run job
        JobClient.runJob(distJobConf)
      }

      // counting log messages by types
      case "1" => {
        val msgTypeCountConf = new JobConf(MessageTypeJob.getClass)
        msgTypeCountConf.setJobName("Number of log messages by types")
        msgTypeCountConf.setJarByClass(this.getClass)

        // set input and output dirs
        FileInputFormat.setInputPaths(msgTypeCountConf, new Path(args(1)))
        FileOutputFormat.setOutputPath(msgTypeCountConf, new Path(config.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
          config.getString(JobsConfigConstants.MESSAGE_TYPE_OUTPUT_DIR)))

        // set mappers reducers and input/output format
        MessageTypeJob(msgTypeCountConf)

        // run job
        JobClient.runJob(msgTypeCountConf)
      }

      case _ => {
        ???
      }
    }
  }