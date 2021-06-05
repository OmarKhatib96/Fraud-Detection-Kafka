/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.util.parsing.json.JSON


case class click(uid:String,timestamp:String,ImpressionId:String)
case class display(uid:String,timestamp:String,ImpressionId:String)
case  class CompteurDisplays(uid:String,impressionId:String,compteur:Int)
//case class CompteurClicks(uid: String,ip:String ,compteur: Int,timestamp:String)
case class CompteurClicks(uid:String,impressionId:String,compteur: Int)








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

  def parseClick(jsonString: String,field:String):String ={
    // On parse
    val jsonMap = JSON.parseFull(jsonString).getOrElse("").asInstanceOf[Map[String, Any]]

    // On extrait
    if( field == "timestamp" ){
      val field_required = jsonMap.get(field).get.asInstanceOf[Double]
      return field_required.toString()

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

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
    val stream2 = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))

    //Pour les clics
    val process_click=stream2.map({y =>click(parseClick(y,"uid"), parseClick(y,"timestamp"),parseClick(y,"impressionId"))}).map({y=> CompteurClicks(y.uid,y.ImpressionId,1) })
    val process1_click=process_click.keyBy(_.uid)
    val compte_click = process1_click.reduce( (acc, occ)  => {CompteurClicks (acc.uid,acc.impressionId, acc.compteur + 1) })

    //Pour les displays
    val process_display=stream.map({y =>display(parseClick(y,"uid"), parseClick(y,"timestamp"),parseClick(y,"impressionId"))}).map({y=> CompteurDisplays(y.uid,y.ImpressionId,1) })
    val process1_display=process_display.keyBy(_.uid)
    val compte_display = process1_display.reduce( (acc, occ)  => {CompteurDisplays (acc.uid,acc.impressionId, acc.compteur + 1) })

      //On join pour calculer le CTR
    val compte_display_joined=compte_display.join(compte_click).where(compte_display=>compte_display.uid).equalTo(compte_click=>compte_click.uid).window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .apply { (e1, e2) => e1.uid+","+e1.compteur/e2.compteur}.print()












    //Displays
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}