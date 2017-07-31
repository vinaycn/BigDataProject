A sneak peek into airbnb
To analyze the vibe of the neighborhood from the available Airbnb listing dataset and to suggest users some relevant listings based on his/her past experience or locality preference is the ideal goal of this project. We achieved this by building a reactive-big data project using Akka and Play framework.
Technologies Used
Play-Framework, Akka, Slick, MySQL, Kafka, Spark Streaming, Spark MLlib, HBase, Apache Zookeeper, HDFS, AngularJS and ChartJS
Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.
Prerequisites
Things you need installed:
1)JDK 8 or higher - http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
2)Scala 2.11.x or higher - https://www.scala-lang.org/download/2.11.8.html
3)Play 2.5.x or higher - https://www.playframework.com/download
4)MySQL Server - https://dev.mysql.com/downloads/
5)Apache Kafka - https://kafka.apache.org/downloads
6)Apache Spark - git clone git://github.com/apache/spark.git
7)Apache hbase - http://www.apache.org/dyn/closer.cgi/hbase/
8)Apache Zookeeper - if you run hbase on a stand-alone mode or sudo distributed mode, you could use the the ZooKeeper that comes out of the box with hbase.
9)Apache hadoop 2.7.3 or higher- http://hadoop.apache.org/releases.html
Installing
Follow the steps in the sequence given below to get the application up and running.
1) git clone https://akashnagesh91@bitbucket.org/akashnagesh91/a-sneak-peek-into-airbnb.git
2) Start HDFS and YARN: ${HADOOP_HOME}/sbin/start-all.sh
Execute the command JPS to check all the Java processes that are running. If you are running hadoop in sudo disributed mode, you should see atleast 5 processes: -> NameNode ->SecondaryNameNode ->DataNode ->NodeManager ->ResourceManager
3) Start HBase : path/to/Hbase/installation/bin/start-hbase.sh Note: We need an instance of zookeeper for distributed synchronization. The start-hbase.sh also automatically starts a local instance of Zookeeper.
4) Start Kafka: path/to/kafka/installation/bin/kafka-server-start.sh path/to/kafka/installation/config/server.properties Note: Kafka also uses the same instance of zookeeper started by hbase.
5) Start MySql server
6) Go to the cloned directory and start the application by: activator run
