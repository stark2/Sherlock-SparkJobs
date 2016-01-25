package sherlock

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
 * Joins two tables in Cassandra and saves the results.
 * Usage: SparkTransformation 
 *
 * Example:
 *    $ spark-submit   --packages com.datastax.spark:spark-cassandra-connector_2.10:1.4.0  \
 *    --class sherlock.SparkTransformation   --master local[*]  \
 *    /home/cloudera/scala-workspace/Sherlock/target/scala-2.10/sherlock_2.10-0.1.jar  \   
 */
object SparkTransformation {
  
  case class Userdata(mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String)
  
  case class Threatdata(raw: String, classification_type: String, event_description_text: String, feed_accuracy: Int, feed_name: String, feed_url: String, malware_name: String,
    source_asn: String, source_fqdn: String, source_ip: String, source_network: String, source_reverse_dns: String, source_url: String, time_observation: Long, time_source:Long)
  
  case class Report(mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String)    

  def toUserdataTuple(line: String) = {
   val x = line.split(",")
   Userdata(x(0), x(1).toLong, x(2), x(3), x(4).toInt, x(5), x(6).toInt, x(7), x(8))
   }
  
  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println(s"""
        |Usage: SparkTransformation 
        |
        """.stripMargin)
      System.exit(1)
    }

    SparkLogging.setStreamingLogLevels()

    val sparkConf = new SparkConf().setAppName("SparkTransformation").set("spark.cassandra.connection.host", "192.168.5.108")
    sparkConf.set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(sparkConf)
    
    val startTime = DateTime.now
    
    CassandraConnector(sparkConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS sherlock WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS sherlock.userdata (mac text, time bigint, protocol text, ip_address_source text, port_source int, ip_address_target text, port_target int, network_status text, service text, PRIMARY KEY (mac, time))")
        session.execute("CREATE TABLE IF NOT EXISTS sherlock.report (mac text, time bigint, protocol text, ip_address_source text, port_source int, ip_address_target text, port_target int, network_status text, service text, PRIMARY KEY (mac, time))")
        //session.execute("TRUNCATE sherlock.userdata")
        //session.execute("TRUNCATE sherlock.report")
      }  

    val u_data = sc.cassandraTable[Userdata]("sherlock", "userdata") //.cache
    val t_data = sc.cassandraTable[Threatdata]("sherlock", "threatdata").cache 
    
    val uDataByIp = u_data.keyBy(f => f.ip_address_target);
    val tDataByIp = t_data.keyBy(f => f.source_ip);
    
    //Join the tables by the IP
    val joinedData = uDataByIp.join(tDataByIp) //.cache
    //Create RDD with a the new object type which maps to the new table
    //Report: mac: String, time: Long, protocol: String, ip_address_source: String, port_source: Int, ip_address_target: String, port_target: Int, network_status: String, service: String
    val reportObjects = joinedData.map(f => (new Report(f._2._1.mac, f._2._1.time, f._2._1.protocol, f._2._1.ip_address_source, f._2._1.port_source, f._2._2.source_ip, f._2._1.port_target, f._2._1.network_status, f._2._1.service))) //.cache
    //save to Cassandra
    reportObjects.saveToCassandra("sherlock", "report")

    
    val endTime = DateTime.now
    val period: Period = new Period(endTime, startTime)
    
    sc.stop()
  }
}
// scalastyle:on println