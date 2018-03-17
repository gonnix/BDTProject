
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes

object SparkWordCount extends Serializable{
  // HBase schema
  final val tableName = "job_info"
  final val cfGeneral = Bytes.toBytes("general")
  final val cfDetails = Bytes.toBytes("details")
  final val colCompany = Bytes.toBytes("company")
  final val colTitle = Bytes.toBytes("title")
  final val colCategory = Bytes.toBytes("category")
  final val colLocation = Bytes.toBytes("location")
  final val colResponsibility = Bytes.toBytes("responsibility")
  final val colMinimum = Bytes.toBytes("minimum")
  final val colPrefer = Bytes.toBytes("prefer")

  case class JobInfo(company:String, title:String, category:String,	location:String,
                     responsibility:String,	minimum:String,	prefer:String)

  object JobInfo extends Serializable{
    def parseJobInfo(str:String): JobInfo = {
      val p = str.split(",")
      JobInfo(p(0), p(1), p(2), p(3), p(4), p(5), p(6))
    }

    def convertToPut(jobinfo: JobInfo): (ImmutableBytesWritable, Put) = {
      val format = new SimpleDateFormat("ddmmyyyyhhmmss")
      val rowkey = jobinfo.company + "_" + format.format(Calendar.getInstance().getTime());
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object
      put.addColumn(cfGeneral, colCompany, Bytes.toBytes(jobinfo.company))
      put.addColumn(cfGeneral, colTitle, Bytes.toBytes(jobinfo.title))
      put.addColumn(cfGeneral, colCategory, Bytes.toBytes(jobinfo.category))
      put.addColumn(cfGeneral, colLocation, Bytes.toBytes(jobinfo.location))
      put.addColumn(cfDetails, colResponsibility, Bytes.toBytes(jobinfo.responsibility))
      put.addColumn(cfDetails, colMinimum, Bytes.toBytes(jobinfo.minimum))
      put.addColumn(cfDetails, colPrefer, Bytes.toBytes(jobinfo.prefer))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }
  }

  def main(args:Array[String]) {
    if(args.length != 1) {
      println("KafkaSparkStreamingHBase <topic>")
      return
    }

    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/cloudera/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //Kafka
    val kafkaConf = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming-example",
      "zookeeper.connection.timeout.ms" -> "1000")

    val lines = KafkaUtils.createStream[Array[Byte], String,
      StringDecoder, StringDecoder](
      ssc,
      kafkaConf,
      Map(args(0) -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    // Parse info from stream
    val jobDStream = lines.map(JobInfo.parseJobInfo)
    jobDStream.print()

    jobDStream.foreachRDD { rdd =>
      rdd.map(JobInfo.convertToPut).
        saveAsHadoopDataset(jobConfig)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}