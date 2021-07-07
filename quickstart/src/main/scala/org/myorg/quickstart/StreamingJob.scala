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

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.util.Properties
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util.ArrayList
import java.util.List
import java.util.concurrent.TimeUnit


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

//    env.readTextFile(filePath= "/tmp/test_flink.txt")


///////////////////////////////////////////////////////////////////////////////////////////////////////////// GET STREAM

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val topics: List[String] = new ArrayList[String]()
    topics.add("clicks")
    topics.add("displays")

    // CONSUME STREAMS AND CONVERT TO JSON WITH WATERMARKS
    val stream = env
      .addSource(new FlinkKafkaConsumer[ObjectNode](topics, new JSONKeyValueDeserializationSchema(false), properties)
        .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[ObjectNode](Duration.ofSeconds(20)))
      )
//    stream.print()

    // FILTER

//     CREATE SINK TO WRITE TO
//    val outputPath = "../eventSink"
//    val config = OutputFileConfig
//      .builder()
//      .withPartPrefix("5min_event")
//      .withPartSuffix(".txt")
//      .build()
//    val sink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
//      .withRollingPolicy(
//        DefaultRollingPolicy.builder()
//          .withRolloverInterval(TimeUnit.MINUTES.toMillis(2)) // closes after X minutes
//          .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) // closes after X minute of inactivity
//          .withMaxPartSize(10 * 1024 * 1024) // Max size of 10 Mo
//          .build())
//      .withOutputFileConfig(config)
//      .build()

//    stream
//      .map(x => x.toString())
//      .addSink(sink)


/////////////////////////////////////////////////////////////////////////////////////////////// EXTRACT DATA FROM STREAM

    def extract_data(x:ObjectNode) : (String, String, String, Int, Int, Int) = {
      // maps to a tuple ('uid', 'display_count', 'click_count')
      (
      x.get("value").get("uid").textValue,
      x.get("value").get("ip").textValue,
      x.get("value").get("impressionId").textValue,
      if (x.get("value").get("eventType").textValue=="display") 1 else 0,
      if (x.get("value").get("eventType").textValue=="click") 1 else 0,
      x.get("value").get("timestamp").intValue
      )
    }
    // FORMAT THE JSON STREAM TO THE DESIRED OUTPUT FOR ALL FILTERS
    val dataStream = stream
      //  maps to a tuple ('uid', 'IP', 'display_count', 'click_count')
        .map(x => extract_data(x))
    //      .map(x => (x.get("value").get("uid"), x.get("value").get("eventType"), 1))
//      .map(x => (
//                 x.get("value").get("uid").textValue,
//                 if (x.get("value").get("eventType").textValue=="display") 1 else 0,
//                 if (x.get("value").get("eventType").textValue=="click") 1 else 0
//                )
//      )

///////////////////////////////////////////////////////////////////////////////////////////////// FILTER FRAUDULENT DATA

    // INSTANTIATE SINK TO SEND FRAUDULENT DATA TO WITH KafkaProducer
    val fraudFilter = new FlinkKafkaProducer[String](
      "fraudulentEvent",                  // target topic
      new SimpleStringSchema(),    // serialization schema
      properties                  // producer config
      //      FlinkKafkaProducer.Semantic.NONE // fault-tolerance  // Does not work
    )

    // FORMAT FILTERED DATA FOR ANALYSIS AND ADD TO SINK
    def format(fraud_type:String, uid:String, display_count:Int, click_count:Int) : String = {
      f"{'fraud_type': $fraud_type, 'uid': $uid , 'display_count': $display_count, 'click_count': $click_count}"
    }

    val windowLength = 2
    val windowDelay = 1
    // FRAUD PATTERN 1 : HIGH CLICK THROUGH RATE, FILTER ABOVE 20 CLICKS IN 10 MINUTES
    // FORM WINDOW KEYED ON UID
    val clickThroughRate = dataStream
      // keep ('uid', 'display_count', 'click_count')
      .map(x => (x._1, x._4, x._5))
      .keyBy(x => x._1) // key on uid
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2, a._3+b._3) )
//      .filter(x => x._3.toFloat >= x._2.toFloat*0.1) // CTR above 0.1
      .filter(x => x._3 > 2*windowLength)
      .map(x => format(fraud_type = "high_CTR", uid = x._1, display_count = x._2, click_count = x._3))
//      .addSink(fraudFilter)

    // FRAUD PATTERN 2 : IP ADDRESS WITH ANORMALLY HIGH NUMBER OF UID ASSOCIATED, ABOVE 50 IN 10 MINUTES
    // FORM WINDOW KEYED ON IP
    val fraudulentIP = dataStream
      // keep ('IP', 'click_count')
      .map(x => (x._2, x._5))
      .keyBy(x => x._1) // key on IP
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2) )
      .filter(x => x._2 > 5*windowLength)
      .map(x => f"{'fraud_type': fraudulent_IP, 'IP': ${x._1} , 'click_count': ${x._2}}")
//      .addSink(fraudFilter)

    // FRAUD PATTERN 3 : REMOVE BANNERS WITH TOO MANY CLICKS
    // FORM WINDOW KEYED ON IMPRESSIONID
    val overused_banner = dataStream
      // keep ('uid', 'display_count', 'click_count', 'timestamp')
      .map(x => (x._3, x._4, x._5, x._6))
      .keyBy(x => x._1) // key on impressionId
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2, a._3+b._3, (a._4).min(b._4)) )
//      .filter(x => x._3 > 0)
//      .map(x => format(fraud_type = "click_without_display", uid = x._1, display_count = x._2, click_count = x._3))
      .map(x => f"{'fraud_type': banner_overused, 'impressionId': ${x._1} , 'display_count': ${x._2} , 'click_count': ${x._3} , 'timestamp': ${x._4}}")
      .addSink(fraudFilter)

///////////////////////////////////////////////////////////////////////////////////////////////// STREAM FRAUDULENT DATA

    // STREAM BACK FRAUDULENT DATA AND PRINT
    val streamFraudulent = env
      .addSource(new FlinkKafkaConsumer[String]("fraudulentEvent", new SimpleStringSchema(), properties))
    streamFraudulent.print()

    // FILTER THE FRAUDULENT DATA
    // user clicks too often on the ads
    // val fraud_case1_too_many_clicks = windowedStreamEvents.filter(nb_clicks>=15%)
    // ...
    // User clicks several time on the same ad
    // same IP address has several users (join on IP ?)
    // clicks without a corresponding display

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}