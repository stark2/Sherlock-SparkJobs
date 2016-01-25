package sherlock

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import org.joda.time.DateTime
import org.joda.time.Period
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.writer.TTLOption
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
 * Consumes messages from a topic in Kafka and saves in Cassandra.
 * Usage: KafkaToCassandra <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ spark-submit   --packages com.datastax.spark:spark-cassandra-connector_2.10:1.4.0  \
 *    --class sherlock.KafkaToCassandra   --master local[*]  \
 *    /home/cloudera/scala-workspace/Sherlock/target/scala-2.10/sherlock_2.10-0.1.jar  \   
 *    localhost:9092,localhost:9093 kafkatest 
 */
object KafkaToCassandra {
  
  case class Userdata(mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String)
  
  /*
  case class Threatdata(raw: String, classification_type: String, event_description_text: String, feed_accuracy: Int, feed_name: String, feed_url: String, malware_name: String,
    source_asn: String, source_fqdn: String, source_ip: String, source_network: String, source_reverse_dns: String, source_url: String, time_observation: Long, time_source:Long)
  
  case class Report(mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String)    
  */
  
  def toUserdataTuple(line: String) = {
   val x = line.split(",")
   Userdata(x(0), x(1).toLong, x(2), x(3), x(4).toInt, x(5), x(6).toInt, x(7), x(8))
   }
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: KafkaToCassandra <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    SparkLogging.setStreamingLogLevels()

    val Array(brokers, topics) = args 

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToCassandra").set("spark.cassandra.connection.host", "192.168.5.108")
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))

    val startTime = DateTime.now
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
   
    CassandraConnector(sparkConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS sherlock WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS sherlock.userdata (mac text, time bigint, protocol text, ip_address_source text, port_source int, ip_address_target text, port_target int, network_status text, service text, PRIMARY KEY (mac, time))")
        session.execute("CREATE TABLE IF NOT EXISTS sherlock.report (mac text, time bigint, protocol text, ip_address_source text, port_source int, ip_address_target text, port_target int, network_status text, service text, PRIMARY KEY (mac, time))")
        //session.execute("TRUNCATE sherlock.userdata")
        //session.execute("TRUNCATE sherlock.report")
      }  

    //val myTable = ssc.cassandraTable[(String, BigInt, String, String, Int, String, Int, String, String)]("sherlock", "userdata")

    //ssc.cassandraTable[Userdata]("sherlock", "userdata").registerAsTable("userdata")
    
    val lines = messages.map(_._2)
    val rows = lines.map(l => toUserdataTuple(l))
    
    rows.saveToCassandra("sherlock", "userdata", SomeColumns("mac", "time", "protocol", "ip_address_source", "port_source", "ip_address_target", "port_target", "network_status", "service"))

    /*
    val joined = rows.joinWithCassandraTable("sherlock", "threatdata", SomeColumns("source_ip"), SomeColumns("source_ip"))
    //joined.saveToCassandra("sherlock", "report", SomeColumns("mac", "time", "protocol", "ip_address_source", "port_source", "ip_address_target", "port_target", "network_status", "service"))
    joined.foreachRDD{ rdd => 
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(rdd.collect.mkString("\n")) 
    }
    */

    /*
    val cassandraRDD = ssc.cassandraTable("sherlock", "threatdata").select("raw", "source_fqdn").where("source_ip = ?", "144.76.162.245")
    val dstream = new ConstantInputDStream(ssc, cassandraRDD)
    
    dstream.foreachRDD{ rdd => 
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(rdd.collect.mkString("\n")) 
    }
    */

    /*
    val u_data = ssc.cassandraTable[Userdata]("sherlock", "userdata") //.cache
    val t_data = ssc.cassandraTable[Threatdata]("sherlock", "threatdata").cache 
    
    val uDataByIp = u_data.keyBy(f => f.ip_address_target);
    val tDataByIp = t_data.keyBy(f => f.source_ip);
    
    //Join the tables by the IP
    val joinedData = uDataByIp.join(tDataByIp) //.cache
    //Create RDD with a the new object type which maps to the new table
    //Report: mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String
    val reportObjects = joinedData.map(f => (new Report(f._2._1.mac, f._2._1.time, f._2._1.protocol, f._2._1.ip_address_source, f._2._1.port_source, f._2._2.source_ip, f._2._1.port_target, f._2._1.network_status, f._2._1.service))) //.cache
    //save to Cassandra
    reportObjects.saveToCassandra("sherlock", "report")
    */

    /*
     //val words = lines.flatMap(_.split(","))
     lines.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => toUserdataTuple(w)).toDF()
      // Register as table
      wordsDataFrame.registerTempTable("userdatatmp")
      // Do word count on table using SQL and print it
      val wordCountsDataFrame = sqlContext.sql("select ip_address_target, count(*) as total from userdatatmp group by ip_address_target")
      
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })
    */
    
    val endTime = DateTime.now
    val period: Period = new Period(endTime, startTime)
    
    //lines.print()
    //val rdd = ssc.cassandraTable("streaming_demo", "key_value").select("key", "value").where("fu = ?", 3)
        
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
// scalastyle:on println