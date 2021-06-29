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
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
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

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val topics: List[String] = new ArrayList[String]()
    topics.add("clicks")
    topics.add("displays")

    // CONSUME STREAMS AND CONVERT TO JSON
    // /!\ Simplestringschema converts command lines to string. Best to transform to json
//    val stream = env
//      .addSource(new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties))
    val stream = env
      .addSource(new FlinkKafkaConsumer[ObjectNode](topics, new JSONKeyValueDeserializationSchema(false), properties)
        .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[ObjectNode](Duration.ofSeconds(20)))
      )

    // add streamFraud with topic "fraud"

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

    // ADD KafkaProducer for fraud




    // FORM WINDOW
    val windowedStreamUID = stream
      .map(x => (x.get("value").get("uid"), x.get("value").get("eventType"), 1))
//      .map(x => (x,1))
//      .keyBy(x => x.get("value").get("uid"))
      .keyBy(x => x._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.minutes(1)))
      .reduce( (a, b) => (a._1, a._2, a._3+b._3) )
      .filter(x => x._3 >= 20)
//  val windowedStreamIP = stream
//    .keyBy(x => x.get("value").get("IP"))
//    .window(SlidingEventTimeWindows.of(Time.minutes(2), Time.minutes(1)))
//
    windowedStreamUID.print()

//    val output_path = "./"
//    windowedStream.writeAsText(output_path: String)
//
    // JOIN
    // windowedStreamEvents = windowedStreamDisplay.join(windowedStreamDisplay, on="uid")

    // FILTER THE FRAUDULENT DATA
    // user clicks too often on the ads
    // val fraud_case1_too_many_clicks = windowedStreamEvents.filter(nb_clicks>=15%)
    // ...
    // User clicks several time on the same ad
    // same IP address has several users (join on IP ?)

//      stream.addSink(sink)


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

//    streamDisplay
//      .flatMap(raw => JsonMethods.parse(raw).toOption)
//      .map(_.extract[Car])
//      .windowAll("uid")
//      .TimeWindow(Time.minutes(1))


// CODE CHARLES SINK
//val sink: StreamingFileSink[String] = StreamingFileSink
//  .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
//  .withRollingPolicy(
//  DefaultRollingPolicy.builder()
//  .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
//  .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//  .withMaxPartSize(1024 * 1024 * 1024)
//  .build())
//  .build()
//
//  stream.addSink(sink)