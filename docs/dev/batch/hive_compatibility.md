---
title: "Hive Compatibility"
is_beta: true
nav-parent_id: batch
nav-pos: 9
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

Because Apache Hive is a de facto standard in SQL batch processing, it’s beneficial for Flink SQL to integrate with Hive as closely as possible. The goal here is not to replace or even replicate Hive. Rather, we leverage Hive as much as we can, and offer an alternative to Hive from Flink
community and expose a similar to but probably more performing SQL tool than Hive to Flink
users. Via Flink SQL, user can enjoy Hive’s rich SQL functionality and flexibility.

Now Flink's Alibaba branch is able to access Hive MetaStore metadata and read hive table.

You can:

- Access Hive table meta data after registering external hive catalog in tableEnvironment.(eg. execute ` show tables` or `describe table xxx ` in SQL Cli when you have config specific hive catalog )
- Regard a external hive table as a normal flink table and query it
- Other features such as store flink meta object info into hive catalog and write data back into hive table are in progress.


This document shows how to connect an existing Hive service with Flink. Please refer to the
[Connecting to other systems]({{ site.baseurl }}/dev/batch/connectors.html) guide for more detailed info.

* This will be replaced by the TOC
{:toc}

### Project Configuration

Support for accessing hive metastore and data is part of the `flink-table`  that is always required when writing Flink table api or sql jobs. The code is located in `org.apache.flink.table.catalog` package.

Support for accessing hive metastore and data is contained in the `flink-connector-hive_${scala.binary.version}` Maven module.
This code resides in the `org.apache.flink.table.catalog.hive`
package and `org.apache.flink.streaming.connectors.hive`.

Add the following dependency to your `pom.xml` if you want to use this fucntion.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-hive{{ site.scala_version_suffix }}</artifactId>
	<version>{{site.version}}</version>
</dependency>
{% endhighlight %}


### Accessing hive metaStore and table data

Environment :    

  Assume all physical machines mentioned can be accessed in your work environment
 * Hadoop Cluster (ipA,ipB,ipC)
 * Hive Home Path: ipD  /home/admin/apache-hive-2.3.4
 * Flink SQL Client Path: ipE /home/admin/flink_sql_client

I first prepared two tables in hive, order_details and  products.

```shell
hive> describe order_details;
OK
orderid               bigint
productid             bigint
unitprice             double
quantity              int
discount              double

hive> describe products;
OK
productid             bigint
productname           string
supplierid            bigint
categoryid            bigint
quantityperunit       string
unitprice             double
unitsinstock          bigint
unitsonorder          bigint
reorderlevel          int
discontinued          int
);


```

Before running Flink SQl Client, we should first start the yarn session mode(on the flink sql client home path): 
1. `bin/yarn-session.sh -n 4 -qu root.default -s 4 -tm 2048 -nm test_session_001`
2. Assume from step1 we get the sessionId is \${appId}, then modify the conf/sql-client-defaults.yaml 的 deployment:下配置：yid: ${appId}
3. Our conf/sql-client-defaults.yaml config are as follows which has set hive catalog as the default catalogs.

```yaml
  execution:
  # 'batch' or 'streaming' execution
    type: batch
  # allow 'event-time' or only 'processing-time' in sources
    time-characteristic: event-time
  # interval in ms for emitting periodic watermarks
    periodic-watermarks-interval: 200
  # 'changelog' or 'table' presentation of results
    result-mode: table
  # parallelism of the program
    parallelism: 1
  # maximum parallelism
    max-parallelism: 12
  # minimum idle state retention in ms
    min-idle-state-retention: 0
  # maximum idle state retention in ms
    max-idle-state-retention: 0

deployment:
  # general cluster communication timeout in ms
  response-timeout: 5000
  # (optional) address from cluster to gateway
  gateway-address: ""
  # (optional) port from cluster to gateway
  gateway-port: 0

  # (optional) yarn cluster per-job mode
  # jobmanager: yarn-cluster

  # (optional) yarn session mode
  yid: application_1543205128210_0045


catalogs:
   - name: myhive
     catalog:
      type: hive
      connector:
        hive.metastore.uris: thrift://ipE:ipE port
        hive.metastore.username: flink
      is-default: true
      default-db: default
   - name: myinmemory
     catalog:
      type: flink_in_memory
```


##### Accessing hive data type and data in sql cli stpes

1. Running Flink SQL Client by execute command `bin/sql-client.sh embedded` 
2. `show catalogs;`  // show all catalogs
3. `show databases;`  // show all dbs in the default catalog
4. `show tables;`  // show all tables in the default db in default catalog
5. `use myhive.default;`   // set the default catalog and db
6. `describe products;`  // describe table schema
7. `describe order_details;`  // describe table schema
8. `select * from products;`
9. `select count(*) from order_details;`  // count
10. `select productid, count(1) as sale_number from order_details group by productid;` // group by count prodcut  sale number
11. join then group by to calc single product sales

```sql 
select 
   t.productid,
   t.productname,
   sum(t.price) as sale
from 
  (select 
      A.productid,
      A.productname as productname, 
        B.unitprice * discount as price
     from
      products as A, order_details as B 
     where A.productid = B.productid) as t
  group by t.productid, t.productname;
  
``` 


