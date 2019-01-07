---
title: "SQL Examples"
nav-title: SQL Examples
nav-parent_id: examples
nav-pos: 25
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

## SQL Client Examples

TODO: depend on SQL Client to be ready. (青陆)

For more information see the [SQL]({{ site.baseurl }}/dev/table/sql.html) and [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html).

## Submit SQL Query Programmatically
SQL queries can be submitted using the `sqlQuery()` method of the TableEnvironment programmatically. 

StreamJoinSQLExample shows the usage of SQL Join on Stream Tables. It computes orders shipped within one hour. The algorithm works in two steps: First, join the Order table and the Shipment table on the orderId field. Second, filter the join result by createTime.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// set up the execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

DataStream<Order> order = env.fromElements(
    new Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
    new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
    new Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
    new Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
    new Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8));
DataStream<Shipment> shipment = env.fromElements(
    new Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
    new Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
    new Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5));

// register the DataStreams under the name "t_order" and "t_shipment"
tEnv.registerDataStream("t_order", order, "createTime, unit, orderId, productId");
tEnv.registerDataStream("t_shipment", shipment, "createTime, orderId");

// run a SQL to get orders whose ship date are within one hour of the order date
Table table = tEnv.sqlQuery(
    "SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
        " FROM t_order AS o" +
        " JOIN t_shipment AS s" +
        "  ON o.orderId = s.orderId" +
        "  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR");

DataStream<Row> resultDataStream = tEnv.toAppendStream(table, Row.class);
resultDataStream.print();

// execute program
env.execute();
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/StreamJoinSQLExample.java  "StreamJoinSQLExample" %} implements the above described algorithm.

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// set up the execution environment
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

val t_order: DataStream[Order] = env.fromCollection(Seq(
  Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
  Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
  Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
  Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
  Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8)))

val t_shipment: DataStream[Shipment] = env.fromCollection(Seq(
  Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
  Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
  Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5)))

// register the DataStreams under the name "t_order" and "t_shipment"
tEnv.registerDataStream("t_order", t_order, 'createTime, 'unit, 'orderId, 'productId)
tEnv.registerDataStream("t_shipment", t_shipment, 'createTime, 'orderId)

// run a SQL to get orders whose ship date are within one hour of the order date
val result = tEnv.sqlQuery(
  "SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
    " FROM t_order AS o" +
    " JOIN t_shipment AS s" +
    "  ON o.orderId = s.orderId" +
    "  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR")

result.toAppendStream[Row].print()

// execute program
env.execute()

// user-defined pojo
case class Order(createTime: Timestamp, unit: Int, orderId: Long, productId: Long)

case class Shipment(createTime: Timestamp, orderId: Long)
    
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala/StreamJoinSQLExample.scala  "StreamJoinSQLExample.scala" %} implements the above described algorithm.

</div>
</div>

To run the StreamJoinSQLExample, issue the following command:

{% highlight bash %}
$ ./bin/flink run ./examples/table/StreamJoinSQLExample.jar
{% endhighlight %}

To see the output log:
{% highlight bash %}
$ tail -f ./log/flink-*-taskexecutor-*.out
2018-10-15 09:05:02.0,9,3,2018-10-15 09:11:00.0
2018-10-15 10:55:01.0,8,5,2018-10-15 11:31:10.0
{% endhighlight %}

{% top %}
