---
title: "Supported SQL DDL Source & Sinks (Not ready)"
nav-parent_id: tableapi
nav-pos: 35
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

Flink SQL provides access to data which is stored in external systems (database, key-value store, message queue) or files.

By CREATE TABLE statement, data could be accessed as a SQL table in the following DML statements and translated to `TableSource` or `TableSink` automatically.

We use WITH clauses to describe the information necessary to access a external system.

Provided Connectors
-------------------

| **Connector Type**| **Batch?** | **Streaming?** | **Source?** | **Sink?**| **Temporal Join?**
| `CSV` | Y | Y | Y | Y | Y

### CSV connector

{% highlight sql %}

-- Create a table named `Orders` which includes a primary key, and is stored as a CSV file
CREATE TABLE Orders (
    orderId BIGINT NOT NULL,
    customId VARCHAR NOT NULL,
    itemId BIGINT NOT NULL,
    totalPrice BIGINT NOT NULL,
    orderTime TIMESTAMP NOT NULL,
    description VARCHAR,
    PRIMARY KEY(orderId)
) WITH (
    type='csv',
    path='file:///abc/csv_file1'
)

{% endhighlight %}

#### Required configuration
* **type** : use `CSV` to create a Csv Table to read CSV files or to write into CSV files.
* **path** : locations of the CSV files.  Accepts standard Hadoop globbing expressions. To read a directory of CSV files, specify a directory.

#### Optional Configuration
* **enumerateNestedFiles** : when set to `true`, reader descends the directory for csv files. By default `true`.
* **fieldDelim** : the field delimiter. By default `,`, but can be set to any character.
* **lineDelim** : the line delimiter. By default `\n`, but can be set to any character.
* **charset** : defaults to `UTF-8`, but can be set to other valid charset names.
* **override** : when set to `true` the existing files are overwritten. By default `false`.
* **emptyColumnAsNull** : when set to `true`, any empty column will be set as null. By default `false`.
* **quoteCharacter** : by default no quote character, but can be set to any character.
* **firstLineAsHeader** : when set to `true`, the first line of files are used to name columns and are not included in data. All types are assumed to be string. By default `false`.
* **parallelism** : the number of files to write to.
* **timeZone** : timeZone to parse DateTime columns. Defaults to `UTC`, but can be set to other valid time zones.
* **commentsPrefix** : skip lines beginning with this character. By default no commentsPrefix, but can be set to any string.
* **updateMode** : the ways to encode a changes of a dynamic table. By default `append`. [See Table to Stream Conversion]({{ site.baseurl }}/dev/table/streaming/dynamic_tables.html#table-to-stream-conversion)
   * **append** : encoding INSERT changes.
   * **upsert** : encoding INSERT and UPDATE changes as upsert message and DELETE changes as delete message.
   * **retract** : encoding INSERT as add message and DELETE changes as retract message,  and an UPDATE change as a retract message for the updated (previous) row and an add message for the updating (new) row.

### HBase Connector
#### Support Matrix
| Mode \ Type | Source | Sink | Temporal Join |
| --- | --- | --- | --- |
| Batch | I | Y | Y |
| Streaming | N | Y | Y |
**Legend**:
- Y: support
- N: not support
- I: incoming soon

{% highlight sql %}
CREATE TABLE testSinkTable (
     ROWKEY BIGINT,
     `` `family1.col1` `` VARCHAR,
     `` `family2.col1` `` INTEGER,
     `` `family2.col2` `` VARCHAR,
     `` `family3.col1` `` DOUBLE,
     `` `family3.col2` `` DATE,
     `` `family3.col3` `` BIGINT,
     PRIMARY KEY(ROWKEY)
) WIHT {
    type='HBASE',
    connector.property-version='1.4.3',
    hbase.zookeeper.quorum='test_hostname:2181'
}

{% endhighlight %}

**Note** : the HBase table schema (that used for writing or temporal joining) must have a single column primary key which named `ROWKEY` and the column name format should be `` `columnFamily.qualifier` ``.

#### Required Configuration
* **type** : use `HBASE` to create an HBase table to read/write data.
* **connector.property-version** : specify the HBase client version, currently only '1.4.3' is available. More version(s) will'be supported later.
* **tableName** : specify the name of the table in HBase.
* **hbase.zookeeper.quorum** : specify the ZooKeeper quorum configuration for accessing the HBase cluster. **Note** : please specify this parameter or ensure a default `hbase-site.xml` is valid in the current classpath.

#### Optional Configuration
* **`hbase.*`** : support all the parameters that have the 'hbase.' prefix, e.g., 'hbase.client.operation.timeout'.

