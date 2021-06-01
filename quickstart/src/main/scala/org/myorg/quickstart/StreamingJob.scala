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
import scala.util.parsing.json.JSON


case class click(  uid:String ,  timestamp:String, ip:String)
case class CompteurMot(uid: String,ip:String ,compteur: Int,timestamp:String)












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

    val splituid2=stream2.map({y => click(parseClick(y,"uid"),parseClick(y,"timestamp"),parseClick(y,"ip"))}).map({y=> CompteurMot(y.uid,y.ip,1,y.timestamp) })
    val splituid3=splituid2.keyBy(_.uid)
    val compte = splituid3.reduce( (acc, occ)  => {CompteurMot (acc.uid,acc.ip, acc.compteur + 1,acc.timestamp) }).print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}