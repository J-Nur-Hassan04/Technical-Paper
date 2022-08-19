# Apache Kafka

**What is Kafka?**

Apache Kafka is a distributed publish-subscribe messaging system and a robust queue that can handle a high volume of data and enables you to pass messages 
from one end-point to another. Kafka is suitable for both offline and online message consumption. Kafka messages are persisted on the disk and replicated 
within the cluster to prevent data loss. Kafka is built on top of the ZooKeeper synchronization service. It integrates very well with Apache Storm and 
Spark for real-time streaming data analysis.

## **Benefits**

**Following are a few benefits of Kafka**

   **Reliability** => Kafka is distributed, partitioned, replicated and fault tolerance.

   **Scalability** => Kafka messaging system scales easily without down time.

   **Durability** => Kafka uses "Distributed commit log" which means messages persists on disk as fast as possible, hence it is durable.

   **Performance** => Kafka has high throughput for both publishing and subscribing messages. It maintains stable performance even many TB of messages are 
   stored.

## Use Cases

**Kafka can be used in many Use Cases. Some of them are listed below**

   **Metrics** − Kafka is often used for operational monitoring data. This involves aggregating statistics from distributed applications to produce 
    centralized feeds of operational data.

   **Log Aggregation Solution** − Kafka can be used across an organization to collect logs from multiple services and make them available in a standard 
    format to multiple con-sumers.

   **Stream Processing** − Popular frameworks such as Storm and Spark Streaming read data from a topic, processes it, and write processed data to a new 
    topic where it becomes available for users and applications. Kafka’s strong durability is also very useful in the context of stream processing.
    
## Need for Kafka

   Kafka is a unified platform for handling all the real-time data feeds. Kafka supports low latency message delivery and gives guarantee for fault tolerance 
   in the presence of machine failures. It has the ability to handle a large number of diverse consumers. Kafka is very fast, performs 2 million writes/sec. 
   Kafka persists all data to the disk, which essentially means that all the writes go to the page cache of the OS (RAM). This makes it very efficient to 
    transfer data from page cache to a network socket.
    
## Fundamentals    
    
##### Components and Description
 	

**Topics**
       
  A stream of messages belonging to a particular category is called a topic. Data is stored in topics.

  Topics are split into partitions. For each topic, Kafka keeps a mini-mum of one partition. Each such partition contains messages in an immutable 
  ordered sequence. A partition is implemented as a set of segment files of equal sizes.


**Partition**
        
Topics may have many partitions, so it can handle an arbitrary amount of data.
	

**Partition offset**
  
  Each partitioned message has a unique sequence id called as "offset".
	

**Replicas of partition**
        
Replicas are nothing but "backups" of a partition. Replicas are never read or write data. They are used to prevent data loss.
 	

**Brokers**
       
 Brokers are simple system responsible for maintaining the pub-lished data. Each broker may have zero or more partitions per topic. Assume, 
 if there are N partitions in a topic and N number of brokers, each broker will have one partition.

 Assume if there are N partitions in a topic and more than N brokers (n + m), the first N broker will have one partition and the next M broker 
 will not have any partition for that particular topic.

 Assume if there are N partitions in a topic and less than N brokers (n-m), each broker will have one or more partition sharing among them. 
 This scenario is not recommended due to unequal load distri-bution among the broker.

 	
**Kafka Cluster**
  Kafka’s having more than one broker are called as Kafka cluster. A Kafka cluster can be expanded without downtime. These clusters are used to 
  manage the persistence and replication of message data.
 	

**Producers**
  
  Producers are the publisher of messages to one or more Kafka topics. Producers send data to Kafka brokers. Every time a producer pub-lishes a 
  message to a broker, the broker simply appends the message to the last segment file. Actually, the message will be appended to a partition. 
  Producer can also send messages to a partition of their choice.
 	

**Consumers**
        
Consumers read data from brokers. Consumers subscribes to one or more topics and consume published messages by pulling data from the brokers.
 	

**Leader**
        
"Leader" is the node responsible for all reads and writes for the given partition. Every partition has one server acting as a leader.
 	

**Follower**
        
Node which follows leader instructions are called as follower. If the leader fails, one of the follower will automatically become the new leader. 
A follower acts as normal consumer, pulls messages and up-dates its own data store.
        
## Cluster Architecture

##### Components and Description

**Broker**
        
Kafka cluster typically consists of multiple brokers to maintain load balance. Kafka brokers are stateless, so they use ZooKeeper for maintaining 
their cluster state. One Kafka broker instance can handle hundreds of thousands of reads and writes per second and each bro-ker can handle TB of 
messages without performance impact. Kafka broker leader election can be done by ZooKeeper. 	

**ZooKeeper**
        
ZooKeeper is used for managing and coordinating Kafka broker. ZooKeeper service is mainly used to notify producer and consumer about the 
presence of any new broker in the Kafka system or failure of the broker in the Kafka system. As per the notification received by the Zookeeper 
regarding presence or failure of the broker then pro-ducer and consumer takes decision and starts coordinating their task with some other broker.


**Producers**

Producers push data to brokers. When the new broker is started, all the producers search it and automatically sends a message to that new broker. 
Kafka producer doesn’t wait for acknowledgements from the broker and sends messages as fast as the broker can handle.
 	

**Consumers**
        
Since Kafka brokers are stateless, which means that the consumer has to maintain how many messages have been consumed by using partition offset. 
If the consumer acknowledges a particular message offset, it implies that the consumer has consumed all prior messages. The consumer issues an 
asynchronous pull request to the broker to have a buffer of bytes ready to consume. The consumers can rewind or skip to any point in a partition 
simply by supplying an offset value. Consumer offset value is notified by ZooKeeper.
        
        
## Workflow of Pub-Sub Messaging

##### Following is the step wise workflow of the Pub-Sub Messaging −

* Producers send message to a topic at regular intervals.

* Kafka broker stores all messages in the partitions configured for that particular topic. It ensures the messages are equally shared between 
 partitions. If the producer sends two messages and there are two partitions, Kafka will store one message in the first partition and the second 
 message in the second partition.

* Consumer subscribes to a specific topic.

* Once the consumer subscribes to a topic, Kafka will provide the current offset of the topic to the consumer and also saves the offset in the 
  Zookeeper ensemble.

* Consumer will request the Kafka in a regular interval (like 100 Ms) for new messages.

* Once Kafka receives the messages from producers, it forwards these messages to the consumers.

* Consumer will receive the message and process it.

* Once the messages are processed, consumer will send an acknowledgement to the Kafka broker.

* Once Kafka receives an acknowledgement, it changes the offset to the new value and updates it in the Zookeeper. Since offsets are maintained 
    in the Zookeeper, the consumer can read next message correctly even during server outrages.

* This above flow will repeat until the consumer stops the request.

* Consumer has the option to rewind/skip to the desired offset of a topic at any time and read all the subsequent messages.


## Workflow of Queue Messaging / Consumer Group

In a queue messaging system instead of a single consumer, a group of consumers having the same "Group ID" will subscribe to a topic. In simple terms, 
consumers subscribing to a topic with same "Group ID" are considered as a single group and the messages are shared among them. Let us check the actual 
workflow of this system.

* Producers send message to a topic in a regular interval.

* Kafka stores all messages in the partitions configured for that particular topic similar to the earlier scenario.

* A single consumer subscribes to a specific topic, assume "Topic-01" with "Group ID" as "Group-1".

* Kafka interacts with the consumer in the same way as Pub-Sub Messaging until new consumer subscribes the same topic, "Topic-01" with the same 
    "Group ID" as "Group-1".

* Once the new consumer arrives, Kafka switches its operation to share mode and shares the data between the two consumers. This sharing will go on 
    until the number of con-sumers reach the number of partition configured for that particular topic.

* Once the number of consumer exceeds the number of partitions, the new consumer will not receive any further message until any one of the existing 
  consumer unsubscribes. This scenario arises because each consumer in Kafka will be assigned a minimum of one partition and once all the partitions 
  are assigned to the existing consumers, the new consumers will have to wait.

* This feature is also called as "Consumer Group". In the same way, Kafka will provide the best of both the systems in a very simple and efficient 
  manner.

## Role of ZooKeeper

A critical dependency of Apache Kafka is Apache Zookeeper, which is a distributed configuration and synchronization service. Zookeeper serves as the 
coordination interface between the Kafka brokers and consumers. The Kafka servers share information via a Zookeeper cluster. Kafka stores basic metadata 
in Zookeeper such as information about topics, brokers, consumer offsets (queue readers) and so on.

Since all the critical information is stored in the Zookeeper and it normally replicates this data across its ensemble, failure of Kafka 
broker / Zookeeper does not affect the state of the Kafka cluster. Kafka will restore the state, once the Zookeeper restarts. This gives zero downtime 
for Kafka. The leader election between the Kafka broker is also done by using Zookeeper in the event of leader failure.

## Apache Kafka - Installation Steps
   [Installation on Linux](https://kafka.apache.org/quickstart)
   
   [Installation on Windows](https://www.conduktor.io/kafka/how-to-install-apache-kafka-on-windows)
   
   [Installation on macOS](https://www.javatpoint.com/installing-apache-kafka-on-macos)
   
   
## Usefull Resorces
[javatpoint](https://www.javatpoint.com/apache-kafka)

[Apache Kafka](https://kafka.apache.org/)

[Github](https://github.com/twitter/hbc)

[tutorialkart](https://www.tutorialkart.com/apache-kafka-tutorial/)

