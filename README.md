## What is Kafka?
<p>
kafka is opensource distributed event streaming platform used to collect, store, process, analyze data feed at scale.
Kafka operates on a publish-subscribe messaging model built around a distributed commit log. Its architecture relies on several core components:
</p>
<ul>
  <li><b>Producers:</b>  Applications or services that create and publish events (records or messages) to Kafka.</li>
  <li><b>Topic:</b> Named categories, feeds, or channels where producers send messages and consumers read them.</li>
  <li><b>Partitions:</b> Subdivisions of a topic that split data across multiple servers, allowing Kafka to scale horizontally and process data in parallel.</li>
  <li><b>Brokers:</b> The individual servers in a Kafka cluster that store topic partitions, handle client requests, and replicate data for fault tolerance.</li>
  <li><b>Consumers and Consumer Groups:</b>
     Applications that subscribe to topics to read data. Consumer groups let multiple instances work together to process partitions in parallel without duplicating effort.
  </li>
  <li><b>Offsets:</b> Unique sequential numbers assigned to messages inside a partition. They let consumers track their exact reading position and replay data when needed.</li>
</ul>

### Kafka Consumer groups and parallel processing
<p>
  kafka Consumer groups is a collection of consumer application that work together to read and process message  from one or more topic
  without duplicating work.
</p>
<ul>
  <b>How Consumer Group Works</b>
  <li>Shared Workloads</li>
  <li>Offset Tracking</li>
  <li>Automatic Rebalancing</li>
  <li>Multiple Independent Group</li>
</ul>
