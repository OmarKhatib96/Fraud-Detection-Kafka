/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http:/ww.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.myorg.quickstart
import scala.math.max

import java.time.Duration

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.parsing.json.JSON
import org.slf4j.LoggerFactory


case class click(uid:String,timestamp:String,IpAddress:String,ImpressionId:String)
case class display(uid:String,timestamp:String,ipAddress:String,ImpressionId:String)
case  class CompteurDisplays(uid:String,timestamp:String,impressionId:String,ipAddress:String,compteur:Int)
case class CompteurClicks(uid:String,timestamp:String,impressionId:String,ipAddress:String,compteur: Int)
case class AverageClicks(uid:String,timestamp:Long,average: Double)



class ClickPerWindow extends WindowFunction[CompteurClicks, AverageClicks, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
                      uid: String,
                      window: TimeWindow,
                      vals: Iterable[CompteurClicks],
                      out: Collector[AverageClicks]): Unit = {



    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (r.timestamp.toDouble.toInt, c._2 + r.compteur))
    val avgTemp = sum

    // emit a SensorReading with the average temperature
    out.collect(AverageClicks(uid, window.getEnd, avgTemp))
  }
}




/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */



object StreamingJob {

  def parseEvent(jsonString: String,field:String):String ={
    // On parse
    val jsonMap = JSON.parseFull(jsonString).getOrElse("").asInstanceOf[Map[String, Any]]

    // On extrait
    if( field == "timestamp" ){
      val field_required = jsonMap.get(field).get.asInstanceOf[Double]
      return field_required.toLong.toString

    }
    else
    {
      val field_required = jsonMap.get(field).get.asInstanceOf[String]
      return field_required

    }
  }







  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.enableCheckpointing(60000) // checkpoint every minute


    //Constants

    val THRESHOLD_CTR=0.4
    val COUNT_BY_WINDOW=10
    val THRESHOLD_UID_PER_IP=1




    class TimestampExtractor extends AssignerWithPeriodicWatermarks[CompteurClicks]  {
      var currentMaxTimestamp: Long = _

      override def extractTimestamp(clicks: CompteurClicks,prevElementTimestamp:Long) = {
        val timestamp=clicks.timestamp.toLong*1000
        currentMaxTimestamp= max(timestamp, currentMaxTimestamp)
        timestamp


      }
      override def getCurrentWatermark(): Watermark = {
        new Watermark(currentMaxTimestamp)
      }
    }

    class TimestampExtractor2 extends AssignerWithPeriodicWatermarks [CompteurDisplays] with Serializable {
      var currentMaxTimestamp: Long = _

      override def extractTimestamp(displays:  CompteurDisplays,prevElementTimestamp:Long) = {
        val timestamp=displays.timestamp.toLong*1000
        currentMaxTimestamp= max(timestamp, currentMaxTimestamp)
        timestamp
      }
      override def getCurrentWatermark(): Watermark = {
        new Watermark(currentMaxTimestamp)

      }
    }


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
    val stream2 = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))


    //Pour les clics
    val process_click=stream2.map({y =>click(parseEvent(y,"uid"), parseEvent(y,"timestamp"),parseEvent(y,"ip"),parseEvent(y,"impressionId"))}).map({y=> CompteurClicks(y.uid,y.timestamp,y.IpAddress,y.ImpressionId,1) })
    val process1_click=process_click.keyBy(_.uid)
    var compte_click = process1_click.reduce( (acc, occ)  => {CompteurClicks (acc.uid,acc.timestamp,acc.ipAddress,acc.impressionId, acc.compteur + 1) })
    compte_click=compte_click.assignTimestampsAndWatermarks(new TimestampExtractor)//defined watermark here


    //Pour les displays
    val process_display=stream.map({y =>display(parseEvent(y,"uid"), parseEvent(y,"timestamp"),parseEvent(y,"ip"),parseEvent(y,"impressionId"))}).map({y=> CompteurDisplays(y.uid,y.timestamp,y.ipAddress,y.ImpressionId,1) })
    val process1_display=process_display.keyBy(_.uid)
    var compte_display = process1_display.reduce( (acc, occ)  => {CompteurDisplays (acc.uid,acc.timestamp,acc.ipAddress,acc.impressionId, acc.compteur + 1) })
    compte_display=compte_display.assignTimestampsAndWatermarks(new TimestampExtractor2)//defined watermark here


    //[Pattern 1]: Nombre de cliques par uid et par adresses ip
    val process_click_ip=compte_click.keyBy(_.ipAddress).window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5))
    var compte_click_ip = process_click_ip.reduce( (acc, occ)  => {CompteurClicks (acc.uid,acc.timestamp,acc.ipAddress,acc.impressionId, acc.compteur + 1) })
    var compte_click_ip_filtered=compte_click_ip.filter(x=>x.compteur>THRESHOLD_UID_PER_IP)


    // [Pattern 2]: Count des cliques par uid sur une fenêtre donnée
    val count_by_window = process1_click
      .timeWindow(Time.seconds(10))
      .apply(new ClickPerWindow).filter(_.average>=COUNT_BY_WINDOW)



    //[Pattern 3]: On join pour calculer le CTR

    val compte_display_joined=compte_display.join(compte_click).where(_.uid).equalTo(_.uid).window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30))).allowedLateness(Time.seconds(5))
    .apply { (e1, e2) => (e1.uid,(e2.compteur).toDouble/(e1.compteur).toDouble)}.filter(x => x._2 >THRESHOLD_CTR)

    //compte_display_joined.print()



    //Logging the pattern results
    compte_display_joined.writeAsText("C:/Users/Omar/Desktop/M2 IASD/patternCTR.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    compte_click_ip_filtered.writeAsText("C:/Users/Omar/Desktop/M2 IASD/ip_uid.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    count_by_window.writeAsText("C:/Users/Omar/Desktop/M2 IASD/count_by_window.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)



    val LOG = LoggerFactory.getLogger(compte_display_joined.getClass)

    //Displays
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}