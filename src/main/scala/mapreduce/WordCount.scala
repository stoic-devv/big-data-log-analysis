package mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.io.IOException
import java.util.{Iterator, StringTokenizer}
import scala.annotation.tailrec

class WordCount

object WordCount:

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  class Map extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Context): Unit = {
      val itr = new StringTokenizer(value.toString())

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken())
        context.write(word, one)
      }
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {

    val result = new IntWritable()
    def reduce(key: Text, values: Iterator[IntWritable],
               context: Context): Unit = {
      var sum = 0
      while (values.hasNext) {
        sum += values.next().get()
      }
      result.set(sum)
      context.write(key, result)
    }

  }

/*  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf, "word count")
    job.setJarByClass(classOf[WordCount])
    job.setMapperClass(classOf[Map])
    //job.setCombinerClass(classOf[Reduce])
    job.setReducerClass(classOf[Reduce])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)
  }*/
  
  


