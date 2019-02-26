---
title: "JDBC Connector"
nav-title: JDBC Sink
nav-parent_id: connectors
nav-pos: 5
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

This connector provides a Sink that writes records to any jdbc system. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-jdbc</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{site.baseurl}}/dev/linking.html)
for information about how to package the program with the libraries for
cluster execution.

#### JDBC Sink

This is how you can create a jdbc sink which by default, sinks to database:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(YOUR_DRIVER)
				.setDBUrl(YOUR_DB_URL)
				.setQuery("INSERT INTO foobar (id) VALUES (?)")
				.finish()

input.addSink(new JDBCSinkFunction(jdbcOutputFormat));

{% endhighlight %}
</div>

#### Required configuration
* **setDrivername** : Set the jdbc driver for the actual database.
* **setDBUrl** : Set the database url.
* **setQuery** : Set the SQL to write to database. The number of question marks needs to be the same as the number of parameters.

#### Optional Configuration
* **setUsername** : database user name.
* **setPassword** : database user password.
* **setBatchInterval** : JDBC sink optimizes batch writing, this parameter controls how many records are flush once. To ensure exactly-once, JDBC sink will flush when snapshot and close by default.
* **setSqlTypes** : Specify the type of field written, See sql.Types. If not specified, it calls PreparedStatement.setObject by default.

             
                                                                                                   


