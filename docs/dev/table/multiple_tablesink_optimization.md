---
title: "Multiple TableSink Optimization"
nav-parent_id: tableapi
nav-pos: 110
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

Multiple TableSinks are needed if we want to emit multiple results to different external storage in a Flink job. It's better if we can define multiple TableSinks without having common operators executed repeatedly.

* This will be replaced by the TOC
{:toc}

### How to define multiple TableSinks And avoid executing common operators repeatedly
The following example shows how to define multiple TableSinks in a Flink job.

**Note**: It is important to enable subsection optimization if there are multiple TableSinks in a job. In the following example, operators to compute revenue for all French customers will be reused if enable subsection optimization.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// enable subsection optimization
TableConfig conf = new TableConfig();
conf.setSubsectionOptimization(true);
// get a BatchTableEnvironment, works for StreamTableEnvironment equivalently
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, conf);

// register Orders table

// compute revenue for all customers from France
Table revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );
// register a Table
tEnv.registerTable("T", revenue);

// define first TableSink
TableSink csvSink1 = new CsvTableSink("/path/to/file1", ...);

// compute customers with high purchasing ability from France
Table result1 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum >= 100000"
    );
// emit result1 to sink1
result1.writeToSink(csvSink1);

// define second TableSink
TableSink csvSink2 = new CsvTableSink("/path/to/file2", ...);

// compute customers with good purchasing ability from France
Table result2 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum < 100000 AND revSum > 20000"
    );
// emit result2 to sink2
result2.writeToSink(csvSink2);

// execute query
tEnv.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// enable subsection optimization
val conf = new TableConfig
conf.setSubsectionOptimization(true)
val tableEnv = TableEnvironment.getTableEnvironment(env, conf)

// register Orders table

// compute revenue for all customers from France
val revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  )
// register a Table
tEnv.registerTable("T", revenue)

// define first TableSink
val csvSink1 = new CsvTableSink("/path/to/file1", ...)

// compute customers with high purchasing ability from France
val result1 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum >= 100000"
    )
// emit result1 to sink1
result1.writeToSink(csvSink1)

// define second TableSink
val csvSink2 = new CsvTableSink("/path/to/file2", ...)

// compute customers with good purchasing ability from France
val result2 = tableEnv.sqlQuery(
    "SELECT * FROM T WHERE revSum < 100000 AND revSum > 20000"
    )
// emit result2 to sink2
result2.writeToSink(csvSink2)

// execute query
tEnv.execute()
{% endhighlight %}
</div>
</div>

{% top %}

### Difference between a Flink job enable subsection optimization and the one not
As it is known to all, Table API and SQL queries are translated to `StreamGraph` no matter whether their input is a streaming or batch input.

If disable subsection optimization, the translation will happen immediately once emit a result `Table` into a `TableSink` by calling `Table.writeToSink()` or `Table.insertInto()`. The emitted `Table` is internally represented as a logical query plan and is translated in two phases:
1. optimize logical plan. The logical plan is a simple RelNode tree which only has one root node.
2. translate physical plan into into a `StreamGraph`.

If enable subsection optimization, the translation will not happen immediately when emit a result `Table` into a `TableSink`. The translation will only happened after `TableEnvironment` has whole picture of the Flink Job by calling `TableEnvironment.execute`. The translation will be completed in three phases:
1. generate logical plan. If there are multiple `TableSink` in the Flink job, it's logical plan is a RelNode DAG instead of a simple RelNode tree. A RelNode DAG has multiple root nodes. A RelNode Dag can be decomposed into multiple RelNode trees. All common subplans are represented as the same RelNode tree in the RelNode DAG.
2. optimize logical plan. Output of optimization is physical plan.
3. translate physical plan into into a `StreamGraph`.

{% top %}


