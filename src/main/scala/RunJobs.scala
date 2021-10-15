import messagetypes.MessageTypeJob
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf}
import utils.ObtainConfigReference

object RunJobs:
  def main(args: Array[String]): Unit = {

    val jobConf = ObtainConfigReference("jobs", "jobs") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    args(0) match {

      // counting log messages by types
      case "0" => {
        val countMessageTypeConf = new JobConf(this.getClass)
        countMessageTypeConf.setJobName("Number of log messages by types")
        countMessageTypeConf.setJarByClass(this.getClass)

        // set input and output dirs
        FileInputFormat.setInputPaths(countMessageTypeConf, new Path(args(1)))
        FileOutputFormat.setOutputPath(countMessageTypeConf, new Path(jobConf.getString("base_output_dir") +
          jobConf.getString("message_type_output_dir")))

        // set mappers reducers and input/output format
        MessageTypeJob(countMessageTypeConf)

        // run job
        JobClient.runJob(countMessageTypeConf)

        // err hadoop error='cannot allocate memory' (errno=12)

      }

      case _ => {
        ???
      }
    }
  }