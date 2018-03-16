
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder

object SparkWordCount {
  def main(args:Array[String]) {
    //if (args.length != 2) {
    //  System.err.println("Usage: NetworkWordCount ")
    //  System.exit(1)
    //}

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming-example",
      "zookeeper.connection.timeout.ms" -> "1000")

    val lines = KafkaUtils.createStream[Array[Byte], String,
      StringDecoder, StringDecoder](
      ssc,
      kafkaConf,
      Map("wordcounttopic" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}