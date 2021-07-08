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

import org.apache.flink.api.common.eventtime.WatermarkStrategy

import java.util.Properties
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.core.fs.Path
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

//     CREATE SINK TO WRITE TO (commented as it is not in use anymore)
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
//
//    stream
//      .map(x => x.toString())
//      .addSink(sink)


/////////////////////////////////////////////////////////////////////////////////////////////// EXTRACT DATA FROM STREAM

    def extract_data(x:ObjectNode) : (String, String, String, Int, Int, Int) = {
      // maps to a tuple ('uid', 'ip', 'impressionid', 'display_count', 'click_count', 'timestamp')
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

///////////////////////////////////////////////////////////////////////////////////////////////// FILTER FRAUDULENT DATA

    // INSTANTIATE SINK TO SEND FRAUDULENT DATA TO WITH KafkaProducer
    val fraudFilter = new FlinkKafkaProducer[String](
      "fraudulentEvent",                  // target topic
      new SimpleStringSchema(),    // serialization schema
      properties                  // producer config
      //      FlinkKafkaProducer.Semantic.NONE // fault-tolerance  // Does not work
    )

    val windowLength = 4
    val windowDelay = 2
    // FRAUD PATTERN 1 : HIGH CLICK PER MINUTE RATE, FILTER ABOVE 20 CLICKS IN 10 MINUTES
    // FORM WINDOW KEYED ON UID
    val clickThroughRate = dataStream
      // keep ('uid', 'display_count', 'click_count')
      .map(x => (x._1, x._4, x._5))
      .keyBy(x => x._1) // key on uid
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2, a._3+b._3) )
//      .filter(x => x._3.toFloat >= x._2.toFloat*0.1) // CTR above 0.1
      .filter(x => x._3 > 2*windowLength)
      .map(x => f"{'fraud_type': high_click_per_min, 'uid': ${x._2} , 'display_count': ${x._2}, 'click_count': ${x._3}}")
//      .map(x => format(fraud_type = "high_CTR", uid = x._1, display_count = x._2, click_count = x._3))
      .addSink(fraudFilter)

    // FRAUD PATTERN 2 : IP ADDRESS WITH ABNORMALLY HIGH NUMBER OF CLICK ASSOCIATED, ABOVE 5 PER MINUTE
    // FORM WINDOW KEYED ON IP
    val fraudulentIP = dataStream
      // keep ('IP', 'click_count')
      .map(x => (x._2, x._5))
      .keyBy(x => x._1) // key on IP
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2) )
      .filter(x => x._2 > 5*windowLength)
      .map(x => f"{'fraud_type': fraudulent_IP, 'IP': ${x._1} , 'click_count': ${x._2}}")
      .addSink(fraudFilter)

    // FRAUD PATTERN 3 : FILTER EVENT WITHOUT DISPLAY
    // FORM WINDOW KEYED ON UID
    val overused_banner = dataStream
      // keep ('uid', 'display_count', 'click_count', 'timestamp', 'ip', 'impressionId')
      .map(x => (x._1, x._4, x._5, x._6, x._2, x._3))
      .keyBy(x => (x._1)) // key on uid
      .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(windowDelay)))
      .reduce( (a, b) => (a._1, a._2+b._2, a._3+b._3, a._4, a._5, a._6) )
      .filter(x => x._2 == 0)
      .map(x => f"{'fraud_type': no_display, 'timestamp': ${x._4} , 'ip': ${x._5}}, 'impressionId': ${x._6} , 'click_count': ${x._3}")
      .addSink(fraudFilter)

///////////////////////////////////////////////////////////////////////////////////////////////// STREAM FRAUDULENT DATA

    // STREAM BACK FRAUDULENT DATA AND PRINT
    val streamFraudulent = env
      .addSource(new FlinkKafkaConsumer[String]("fraudulentEvent", new SimpleStringSchema(), properties))
    streamFraudulent.print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}