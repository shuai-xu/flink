---
title:  "Flink Interpreter for Apache Zeppelin"
nav-title: Flink on Zeppelin
nav-parent_id: ops
nav-pos: 11
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

<style>
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

/* Move down content because we have a fixed navbar that is 60px tall */
@import url(//fonts.googleapis.com/css?family=Patua+One);
@import url(//fonts.googleapis.com/css?family=Open+Sans);

body {
  padding-top: 50px;
  padding-bottom: 20px;
  line-height: 1.6;
  color: #4c555a;
  letter-spacing: .2px;
  font-size: 15px;
}

.jumbotron {
  background-color: #3071a9;
}

.jumbotron h1,
.jumbotron p {
  color: #fff;
}

.jumbotron p {
  font-size: 19px;
}

.jumbotron .btn {
  font-size: 16px;
}

.jumbotron .thumbnail {
  margin-top: 0;
}

.jumbotron.small {
  padding: 0 0 0 0;
  color: #ffffff;
}

.jumbotron.small .title{
  float: left;
  font-weight: bold;
  font-size: 20px;
  height: 30px;
  margin-right: 20px;
}

.jumbotron.small .subtitle{
  font-size: 14px;
  height: 30px;
  vertical-align: text-bottom;
  padding-top: 7px;
}

.jumbotron.small .description{
  margin-top: 7px;
}

.jumbotron h1 {
  font-family: 'Patua One', cursive;
}

.jumbotron h1 {
  font-family: 'Patua One', cursive;
}

.jumbotron small {
  font-size: 60%;
  color: #FFF;
}

.jumbotron small {
  font-size: 60%;
  color: #FFF;
}

.navbar-brand {
  padding-top: 15px;
  padding-bottom: 15px;
}

.navbar-brand-main img {
  margin: 0;
}

.navbar {
  background: #3071a9;
  border-bottom: 0px;
  height: 60px;
  box-shadow: 0px 3px 6px rgba(0, 0, 0, 0.4);
}

.navbar-inverse .navbar-nav > li > a {
  padding-top: 18px;
  padding-bottom: 20px;
  font-size: 15px;
  font-weight: 300;
  color: white;
  background: #3071a9;
}

.navbar-inverse .navbar-nav > li > a:hover {
  color: white !important;
  background: #2C6094 !important;
}

.navbar-inverse .navbar-collapse.in .navbar-nav .dropdown-menu > li > a:hover,
.navbar-inverse .navbar-collapse.in .navbar-nav .dropdown-menu > li > a:focus {
  color: white !important;
  background: #2C6094 !important;
}

.navbar-inverse .navbar-collapse.in .navbar-nav .dropdown-menu > li > a {
  color: white !important;
}

.navbar-inverse .navbar-collapse.in .navbar-nav .divider {
  background: #286090;
}

.navbar-inverse .navbar-nav > li > a.active:hover,
.navbar-inverse .navbar-nav > li > a.active:focus {
  text-decoration: none;
  background: #265380;
}

.navbar-inverse .navbar-nav > li > a.active {
  background: #265380;
}

.navbar-inverse .navbar-brand-main {
  color: white;
  text-decoration: none;
  font-size: 32px;
}

.navbar-inverse .navbar-collapse,
.navbar-inverse .navbar-form {
  border-color: #265380;
  background: #3071a9;
}

.scrollable-menu {
  max-height: 500px;
  overflow: auto;
}

.scrollable-menu::-webkit-scrollbar {
  -webkit-appearance: none;
  width: 7px;
}

.scrollable-menu::-webkit-scrollbar-thumb {
  border-radius: 3px;
  background-color: gray;
}

.index-header {
  font-size: 16px;
  font-style: italic;
  margin-bottom: 15px;
  margin-top: 15px;
}

.index-description {
  line-height: 1.6;
  padding: 10px;
}

@media (max-width: 768px) {
  .navbar-collapse.in {
    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.4);
  }

  .bigFingerButton {
    margin-top: 12px;
    display: block;
    margin-right: auto;
    margin-left: auto;
  }
}

.bigFingerButton {
  margin-right: 10px;
}

.navbar-inverse .navbar-toggle {
  margin-top: 12px;
  border-color: transparent;
}

.navbar-inverse .navbar-toggle:focus {
  outline-width: 0;
}

.navbar-inverse .navbar-toggle:hover,
.navbar-inverse .navbar-toggle:focus {
  border-color: #265380;
  background: #265380;
}

.navbar-inverse .navbar-toggle.collapsed {
  border-color: #3071A9;
  background: #3071a9;
}

/* CUSTOMIZE THE CAROUSEL
-------------------------------------------------- */

/* Carousel base class */
.carousel {
  height: 200px;
  margin-bottom: 10px;
}
/* Since positioning the image, we need to help out the caption */
.carousel-caption {
  z-index: 10;
}

/* Declare heights because of positioning of img element */
.carousel .item {
  height: 300px;
}

.carousel-control {
  background-image: none !important;
}

.carousel-inner > .item > img {
  position: absolute;
  top: 0;
  left: 0;
  min-width: 100%;
  height: 300px;
}

.carousel-indicators {
  margin-top: 30px;
  margin-bottom: 0;
}

@media screen and (min-width: 768px) {
  .carousel-indicators {
    margin-bottom: -60px;
  }
  .carousel-caption {
    padding-bottom: 60px;
  }
}

/* screenshot img inside of doc */
.screenshot {
  width: 800px;
}

.rotate270 {
  width: 15px;
  padding: 10px 0 0 0;
  -webkit-transform: rotate(270deg);
  -moz-transform: rotate(270deg);
  -ms-transform: rotate(270deg);
  -o-transform: rotate(270deg);
  transform: rotate(270deg);
}

/* Custom container */
.content {
  word-wrap: break-word;
}

.content :first-child {
  margin-top: 20px;
}

@media screen and (min-width: 64em) {
  .content {
    max-width: 64em;
    padding: 2rem 6rem;
    margin: 0 auto;
  }
}
@media screen and (min-width: 42em) and (max-width: 64em) {
  .content {
    padding: 2rem 4rem;
  }
}
@media screen and (max-width: 42em) {
  .content {
    padding: 2rem 1rem;
  }
}

/* <a> */
.content a {
  color: #4183C4;
}
a.absent {
  color: #cc0000;
}
a.anchor {
  display: block;
  padding-left: 30px;
  margin-left: -30px;
  cursor: pointer;
  position: absolute;
  top: 0;
  left: 0;
  bottom: 0;
}

/* <hn> */
.content h1, h2, h3, h4, h5, h6 {
  font-family: "Roboto", "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
  margin-top: 3rem;
  margin-bottom: 1rem;
  font-weight: bold;
  color: rgba(21,21,21,0.8);
}
.content h1 {
  font-size: 30px;
  color: black;
}
.content h2 {
  font-size: 28px;
  padding-top: 5px;
  padding-bottom: 5px;
  border-bottom: 1px solid #E5E5E5;
}
.content h3 {
  font-size: 22px;
  padding-top: 5px;
  padding-bottom: 5px;
}
.content h4 {
  font-size: 18px;
}
.content h5 {
  font-size: 14px;
}
.content h6 {
  font-size: 14px;
  color: #777777;
}

.content img {
  max-width: 100%;
}

/* <li, ul, ol> */
.content li {
  margin: 0;
}
.content li p.first {
  display: inline-block;
}
.content ul :first-child, ol :first-child {
  margin-top: 0px;
}

.content .nav-tabs {
  margin-bottom: 10px;
}

/* <code> */
.content code {
  padding: 2px 4px;
  font-family: Consolas, "Liberation Mono", Menlo, Courier, monospace;
  font-size: 90%;
  color: #567482;
  background-color: #f3f6fa;
  border-radius: 0.3rem;
}

/* <pre> */
.content pre {
  padding: 0.8rem;
  margin-top: 0;
  margin-bottom: 1rem;
  font-family: Consolas, "Liberation Mono", Menlo, Courier, monospace;
  font-size: 90%;
  color: #567482;
  word-wrap: normal;
  background-color: #f3f6fa;
  border: solid 1px #dce6f0;
  border-radius: 0.3rem;
}
.content pre > code {
  padding: 0;
  margin: 0;
  font-size: 95%;
  color: #567482;
  word-break: normal;
  white-space: pre;
  background: transparent;
  border: 0;
}
.content .highlight {
  margin-bottom: 1rem;
}
.content .highlight pre {
  margin-bottom: 0;
  word-break: normal;
}
.content .highlight pre,
.content pre {
  padding: 0.8rem;
  overflow: auto;
  font-size: 90%;
  line-height: 1.45;
  border-radius: 0.3rem;
  -webkit-overflow-scrolling: touch;
}
.content pre code,
.content pre tt {
  display: inline;
  max-width: initial;
  padding: 0;
  margin: 0;
  overflow: initial;
  line-height: inherit;
  word-wrap: normal;
  background-color: transparent;
  border: 0;
}
.content pre code:before, .main-content pre code:after,
.content pre tt:before,
.content pre tt:after {
  content: normal;
}

/* <blockquotes> */
.content blockquote {
  padding: 0 1rem;
  margin-left: 0;
  color: #819198;
  border-left: 0.3rem solid #dce6f0;
}
.content blockquote > :first-child {
  margin-top: 0;
}
.content blockquote > :last-child {
  margin-bottom: 0;
}
.content blockquote p {
  font-size: 14px;
}

/* <table> */
.content table {
  display: block;
  width: 100%;
  word-break: normal;
  word-break: keep-all;
  -webkit-overflow-scrolling: touch;
  font-size: 87%;
  margin-top: 16px;
  margin-bottom: 16px;
}
.content table th {
  font-weight: bold;
  text-align: center;
  background-color: rgba(91, 138, 179, 0.10);
}
.content table th,
.content table td {
  padding: 0.7rem 1rem;
  border: 1px solid #e9ebec;
}

.properties {
  font-size: 12.5px !important;
  font-weight: normal;
  color: #4c555a !important;
  margin-bottom: 0px;
}



/* posts index */
.post > h3.title {
  position: relative;
  padding-top: 10px;
}

.post > h3.title span.date {
  position: absolute;
  right: 0;
  font-size: 0.9em;
}

.post > .more {
  margin: 10px 0;
  text-align: left;
}

/* post-full*/
.post-full .date {
  margin-bottom: 20px;
  font-weight: bold;
}

/* tag_box */
.tag_box {
  list-style: none;
  margin: 0;
  overflow: hidden;
}

.tag_box li {
  line-height: 28px;
}

.tag_box li i {
  opacity: 0.9;
}

.tag_box.inline li {
  float: left;
}

.tag_box a {
  padding: 3px 6px;
  margin: 2px;
  background: #eee;
  color: #555;
  border-radius: 3px;
  text-decoration: none;
  border: 1px dashed #cccccc;
}

.tag_box a span {
  vertical-align: super;
  font-size: 0.8em;
}

.tag_box a:hover {
  background: #e5e5e5;
}

.tag_box a.active {
  background: #57A957;
  border: 1px solid #4c964d;
  color: white;
}

.navbar-brand-main {
  font-family: 'Patua One', cursive;
}

.navbar-collapse.collapse {
  max-height: 60px;
}

.navbar-inverse .navbar-nav a .caret,
.navbar-inverse .navbar-nav a:hover .caret {
  margin-left: 4px;
  border-top-color: white;
  border-bottom-color: white;
}

.navbar-inverse .navbar-nav > .open > a,
.navbar-inverse .navbar-nav > .open > a:hover,
.navbar-inverse .navbar-nav > .open > a:focus {
  color: white;
  background: #286090;
}

a.anchorjs-link:hover { text-decoration: none; }

/* Table of Contents(TOC) */
#toc {
  padding-top: 12px;
  padding-bottom: 12px;
}

#toc ul {
  margin-left: -14px;
}

#toc ul ul {
  margin-left: -18px;
}

/* Search Page */
#search p {
  font-size: 30px;
  font-weight: bold;
  color: black;
}

#search_results p {
  font-size: 13px;
  font-weight: 400;
}

#search_results a {
  vertical-align: super;
  font-size: 16px;
  text-decoration: underline;
}

#search_results .link {
  font-size: 13px;
  color: #008000;
  padding-bottom: 3px;
}

.navbar-inverse .navbar-brand {
  color: white;
  text-decoration: none;
  font-size: 32px;
  font-size: 27px;
}

/* master branch docs dropdown menu */
#menu .dropdown-menu li span {
  font-family: "Roboto", "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
  padding: 12px 10px 12px 20px;
  font-size: 16px;
  font-weight: 300;
}

#menu .dropdown-menu li a {
  padding-left: 30px;
}

#menu .title {
  color: #3071a9;
}

#menu .caret {
  border-top-color: white;
  border-bottom-color: white;
}

#menu .open .caret {
  border-top-color: #428bca;
  border-bottom-color: #428bca;
}

#menu .navbar-brand-version {
  margin-right: 30px;
  text-decoration: none !important;
}

#menu .navbar-brand-version span {
  float: none;
  display: inline-block;
  vertical-align: bottom;
}

#menu .navbar-brand-version {
  font-size: 14px;
  color: white;
}

/* gh-pages branch docs dropdown menu */
#docs .dropdown-menu {
  left: 0;
  right: auto;
}

#docs .dropdown-menu li span {
  padding: 3px 10px 10px 10px;
  font-size: 13px;
}

/* Custom, iPhone Retina */
@media only screen and (max-width: 480px) {
  .jumbotron h1 {
    display: none;
  }

  .navbar-brand-version {
    display: none;
    color: white;
  }
}

@media only screen and (max-width: 480px) {
  #menu .title {
    color: #bbb;
  }
}

/* when navigation toggle is enabled due to browser size */
@media only screen and (max-width: 768px) {
  #menu .title {
    text-shadow: 1px 2px #353131;
    color: white;
    font-family: "Roboto", "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 16px;
    font-weight: 300;
  }

  .content {
    padding-left: 30px;
    padding-right: 30px;
  }

  .navbar .navbar-brand-main {
    padding-bottom: 0;
  }

  .navbar-nav {
    margin-top: 0px;
  }

  .navbar-inverse .navbar-collapse, .navbar-inverse .navbar-form {
    border-color: #73a0cd;
  }
}

@media only screen and (min-width: 768px) and (max-width: 1024px) {

  .navbar-brand-version {
    display: none;
  }

  .navbar-collapse.collapse {
    padding-right: 0;
  }

  .navbar-fixed-top > .container {
    width: auto;
  }
}


</style>
<div id="toc"></div>

## Overview
[Apache Flink](https://flink.apache.org) is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.


## How to configure Flink interpreter

Here's a list of properties that could be configured to customize Flink interpreter.

<table class="table-configuration">
  <tr>
    <th>Property</th>
    <th>Default value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>flink.execution.mode</td>
    <td>local</td>
    <td>execution mode of flink. It could be local, yarn or remote</td>
  </tr>
  <tr>
    <td>flink.execution.remote.host</td>
    <td></td>
    <td>host name of job manager in remote mode</td>
  </tr>
  <tr>
    <td>flink.execution.remote.port</td>
    <td></td>
    <td>port of job manager rest service in remote mode</td>
  </tr>
  <tr>
    <td>flink.yarn.appName</td>
    <td></td>
    <td>Yarn app name of flink session</td>
  </tr>
  <tr>
    <td>flink.yarn.jm.memory</td>
    <td>1024</td>
    <td>Memory(mb) of JobManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.memory</td>
    <td>1024</td>
    <td>Memory(mb) of TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.num</td>
    <td>2</td>
    <td>Number of TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.slot</td>
    <td>1</td>
    <td>Slot number per TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.queue</td>
    <td>default</td>
    <td>Queue name for yarn app</td>
  </tr>  
  <tr>
    <td>zeppelin.flink.printREPLOutput</td>
    <td>true</td>
    <td>Whether to print repl output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.maxResult</td>
    <td>1000</td>
    <td>Max rows of result for batch Sql output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentBatchSql</td>
    <td>10</td>
    <td>Max number of batch sql executed concurrently</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentStreamSql</td>
    <td>10</td>
    <td>Max number of stream sql executed concurrently</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.scala.color</td>
    <td>true</td>
    <td>Whether to enable color output of Scala Shell</td>
  </tr>                     
</table>

Besides these properties, you can also configure any flink properties that will override the value in `flink-conf.yaml`.
For more information about Flink configuration, you can find it [here](config.html).

### Run Flink in local mode

By default, Flink interpreter run in local mode as the default value of `flink.execution.mode` is `local`.
In local mode, Flink will launch one MiniCluster which include JobManager and TaskManagers in one JVM. But you can still customize the MiniCluster via the following properties:

* `local.number-taskmanage` This property specify how many TaskManagers in MiniCluster.
* `taskmanager.numberOfTaskSlot` This property specify how many slots for each TaskManager. By default it is 1.

### Run Flink in yarn mode

If you want to run Flink in yarn mode, you have to set the following properties:

* `flink.execution.mode` to be `yarn`
* `HADOOP_CONF_DIR` must be specified either in `zeppelin-env.sh` or in interpreter properties.

You can also customize the yarn mode via the following properties:

* `flink.yarn.jm.memory` Memory of JobManager
* `flink.yarn.tm.memory` Memory of TaskManager
* `flink.yarn.tm.num` Number of TaskManager
* `flink.yarn.tm.slot` Slot number per TaskManager
* `flink.yarn.queue` Queue name of yarn app

You have to set `query.proxy.ports` and `query.server.ports` to be a port range otherwise it is impossible to launch multiple TaskManager in one machine.

### Run Flink in standalone mode

If you want to run Flink in standalone mode, you have to set the following properties:

* `flink.execution.mode` to be `remote`
* `flink.execution.remote.host` to be the host name of JobManager
* `flink.execution.remote.port` to be the port of rest server of JobManager

## What can Flink Interpreter do

Zeppelin's Flink interpreter support 3 kinds of interpreter:

* `%flink` (FlinkScalaInterpreter, Run scala code)
* `%flink.bsql` (FlinkBatchSqlInterpreter, Run flink batch sql)
* `%flink.ssql` (FlinkStreamSqlInterpreter, Run flink stream sql)

### FlinkScalaInterpreter(`%flink`)

FlinkScalaInterpreter allow user to run scala code in zeppelin. 4 variables are created for users:

* senv  (StreamExecutionEnvironment)
* benv  (ExecutionEnvironment)
* stenv (StreamTableEnvironment)
* btenv (BatchTableEnvironment)

Users can use these variables to run DataSet/DataStream/BatchTable/StreamTable related job.

e.g. The following code snippet use `benv` to run a batch style WordCount

```scala
%flink

val data = benv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .groupBy(0)
  .sum(1)
  .print()
```

The following use `senv` to run a stream style WordCount

```scala
%flink

val data = senv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .keyBy(0)
  .sum(1)
  .print

senv.execute()
```

### FlinkBatchSqlInterpreter(`%flink.bsql`)

`FlinkBatchSqlInterpreter` support to run sql to query tables registered in `BatchTableEnvironment`(btenv).

e.g. We can query the `wc` table which is registered in scala code.

```scala
%flink

val data = senv.fromElements("hello world", "hello flink", "hello hadoop").
    flatMap(line => line.split("\\s")).
    map(w => (w, 1))

btenv.registerOrReplaceBoundedStream("wc", 
    data, 
    'word,'number)

```

```sql

%flink.bsql

select word, sum(number) as c from wc group by word 

```

### FlinkStreamSqlInterpreter(`%flink.ssql`)

Flink Interpreter also support stream sql via FlinkStreamSqlInterpreter(`%flink.ssql`) and also visualize the streaming data.

Overall there're 3 kinds of streaming sql supported by `%flink.ssql`:

* SingleRow
* Retract
* TimeSeries

#### SingleRow

This kind of sql only return one row of data, but this row will be updated continually. Usually this is used for tracking the aggregation result of some metrics. e.g.
total page view, total transactions and etc. Regarding this kind of sql, you can visualize it via html. Here's one example which calculate the total page view and visualize it via html.

```sql

%flink.ssql(type=single, parallelism=1, refreshInterval=3000, template=<h1>{1}</h1> until <h2>{0}</h2>, enableSavePoint=true, runWithSavePoint=true)

select max(rowtime), count(1) from log
```

<img src="{{BASE_PATH}}/page/img/zeppelin/flink_total_pv.png" width="700"/>

#### Retract

This kind of sql will return a fixed number of rows, but will be updated continually. Usually this is used for tracking the aggregation result of some metrics by some dimensions.
e.g. total page view per page, total transaction per country and etc. Regarding this kind of sql, you can visualize it via the built-in visualization charts of Zeppelin, such as barchart, linechart and etc.
Here's one example which calculate the total page view per page and visualize it via barchart.

```sql
%flink.ssql(type=retract, refreshInterval=2000, parallelism=2, enableSavePoint=true, runWithSavePoint=true)

select 
    url, 
    count(1) as pv
from log 
    group by url
```

<img src="{{BASE_PATH}}/page/img/zeppelin/flink_pv_per_page.png" width="700"/>

#### TimeSeries

This kind of sql will return a fixed number of rows regularly in timeseries. This is usually used for tracking metrics by time window.
e.g. Here's one example which calculate the page view for each 5 seconds window.

```sql
%flink.ssql(type=ts, refreshInterval=2000, enableSavePoint=false, runWithSavePoint=false, threshold=60000)

select
    TUMBLE_START(rowtime, INTERVAL '5' SECOND) as start_time,
    url,
    count(1) as pv
from log
    group by TUMBLE(rowtime, INTERVAL '5' SECOND), url
```
<img src="{{BASE_PATH}}/page/img/zeppelin/flink_pv_ts.png" />


#### Local Properties to customize Flink Stream Sql

Here's a list of properties that you can use to customize Flink stream sql

<table class="table-configuration">
  <tr>
    <th>Property</th>
    <th>Default value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>type</td>
    <td></td>
    <td>single | retract | ts</td>
  </tr>
  <tr>
    <td>refreshInterval</td>
    <td>3000</td>
    <td>How oftern to refresh the result, it is in milliseconds.</td>
  </tr>
    <tr>
      <td>template</td>
      <td>{0}</td>
      <td>This is used for display the result of type singlerow. `{i}` represent the placehold of the `ith` field. You can also use html in the template, such as &lt;h1&gt;{0}&lt;/h1&gt;</td>
    </tr>
  <tr>
    <td>parallelism</td>
    <td></td>
    <td>The parallelism of this stream sql job</td>
  </tr>
  <tr>
    <td>enableSavePoint</td>
    <td>false</td>
    <td>Whether do savepoint when canceling job</td>
  </tr>
  <tr>
    <td>runWithSavePoint</td>
    <td>false</td>
    <td>Whether to run job from savepoint</td>
  </tr>
  <tr>
    <td>threshold</td>
    <td>3600000</td>
    <td>How much history data to keep for TimeSeries StreamJob, 1 hour by default</td>
  </tr>           
</table>


### Other Features

* Job Canceling
    - User can cancel job via the job cancel button
* Flink Job url association
    - Zeppelin will display the job url in paragraph
* Code completion
    - Like other interpreters, user can use `tab` for code completion
* ZeppelinContext
    - Flink interpreter also integrates ZeppelinContext. For how to use ZeppelinContext, please refer this [link](http://zeppelin.apache.org/docs/0.8.0/usage/other_features/zeppelin_context.html).
   
## FAQ

* Most of time, you will get clear error message when some unexpected happens. But you can still check the interpreter log in case the error message in frontend is not clear to you.
  The flink interpreter log is located in `ZEPPELIN_HOME/logs`
