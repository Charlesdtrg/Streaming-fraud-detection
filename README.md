# StreamingFraudDetection
Fraud detection | Streaming data | Kafka | Flink

Project for the course Streaming Data 2020-2021 - Université Paris Dauphine PSL

Authors : Maxime Millavet & Charles de Trogoff

The objective of this project is to create a Flink application to read events 
consisting in ad display and ad clicks and detect fraudulent behaviours. 
3 fraud patterns are present and need to be identified. 
Info on the fraudulent event is then fed back as a stream.

This project contains two main files :  
- the offline_analysis.ipynb notebook were we did an offline analysis of a 2 hours window 
  of the stream data, saved in the static.csv file
- the src\main\scala\org\myorg\quickstart\StreamingJob.scala where we implemented the different filters 
and sent the fraudulent data to a new Kafka topic 'fraudulentEvent'
  
In order to launch the project it is only necessary to launch the StreamingJob file which will stream 
the fraudulent data in your IDE.

Notable other modifications were made in the docker-compose file to add the 'fraudulentEvent' topic and 
in the pom.xml file to add dependencies for bug fixing.
