package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, SequenceFileOutputFormat, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.lib.output

import java.util.{Iterator, StringTokenizer}
import scala.annotation.tailrec

class WordCount

object WordCount:

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      val itr = new StringTokenizer(value.toString)

      /*@tailrec def collect(itr: StringTokenizer): Unit = itr.hasMoreTokens() match {
        case true => {
          word.set(itr.nextToken())
          output.collect(word, one)
        }
      }

      collect(new StringTokenizer(value.toString))*/


      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken())
        output.collect(word, one)
      }
    }
  }

  class Reduce extends MapReduceBase with Reducer[Text,IntWritable,Text,IntWritable] {

    def reduce(key: Text, values: Iterator[IntWritable],
               output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      var sum = 0
      while (values.hasNext) {
        sum += values.next().get()
      }
      output.collect(key, new IntWritable(sum))
    }

  }

  /*def main(args: Array[String]): Unit = {
    val conf = new JobConf(this.getClass)
    conf.setJobName("my word count")

    conf.setJarByClass(this.getClass)

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)

  }*/
  
  


