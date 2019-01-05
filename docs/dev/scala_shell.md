---
title: "Scala REPL"
nav-parent_id: start
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

Flink comes with an integrated interactive Scala Shell.
It can be used in a local setup as well as in a cluster setup.

To use the shell with an integrated Flink cluster just execute:

{% highlight bash %}
bin/start-scala-shell.sh local
{% endhighlight %}

in the root directory of your binary Flink directory. To run the Shell on a
cluster, please see the Setup section below.

## Usage

The shell supports Batch and Streaming.
Two different ExecutionEnvironments are automatically prebound after startup.
Use "benv" and "senv" to access the Batch and Streaming environment respectively.

### DataSet API

The following example will execute the wordcount program in the Scala shell:

{% highlight scala %}
Scala-Flink> val text = benv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala-Flink> val counts = text
    .flatMap { _.toLowerCase.split("\\W+") }
    .map { (_, 1) }.groupBy(0).sum(1)
Scala-Flink> counts.print()
{% endhighlight %}

The print() command will automatically send the specified tasks to the JobManager for execution and will show the result of the computation in the terminal.

It is possible to write results to a file. However, in this case you need to call `execute`, to run your program:

{% highlight scala %}
Scala-Flink> benv.execute("MyProgram")
{% endhighlight %}

### DataStream API

Similar to the batch program above, we can execute a streaming program through the DataStream API:

{% highlight scala %}
Scala-Flink> val textStreaming = senv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala-Flink> val countsStreaming = textStreaming
    .flatMap { _.toLowerCase.split("\\W+") }
    .map { (_, 1) }.keyBy(0).sum(1)
Scala-Flink> countsStreaming.print()
Scala-Flink> senv.execute("Streaming Wordcount")
{% endhighlight %}

Note, that in the Streaming case, the print operation does not trigger execution directly.

The Flink Shell comes with command history and auto-completion.

### Table API

Scala shell also support Table API. Users can execute a streaming Table program with `stenv` and 
a batch Table program with `btenv`. 

The `Table` API supports interactive programming, which allows users to cache an intermediate 
table for later usage. For example, in the following scala shell command sequence, table `t1` 
is cached and the result may be reused in later code.

{% highlight scala %}
scala> val data = Seq(
    ("US", "Red", 10),
    ("UK", "Blue", 20),
    ("CN", "Yellow", 30),
    ("US", "Blue",40),
    ("UK","Red", 50),
    ("CN", "Red",60),
    ("US", "Yellow", 70),
    ("UK", "Yellow", 80),
    ("CN", "Blue", 90),
    ("US", "Blue", 100)
  )

scala> val t = btenv.fromCollection(data).as ('country, 'color, 'amount)
scala> val t1 = t.filter('amount < 100)
scala> t1.cache
scala> val x = t1.print

scala> val t2 = t1.groupBy('country).select('country, 'amount.sum as 'sum)
scala> val res2 = t2.print

scala> val t3 = t1.groupBy('color).select('color, 'amount.avg as 'avg)
scala> val res3 = t3.print
{% endhighlight %}

Note: The cached tables will be cleaned up when the scala shell exit.

## Adding external dependencies

It is possible to add external classpaths to the Scala-shell. These will be sent to the Jobmanager automatically alongside your shell program, when calling execute.

Use the parameter `-a <path/to/jar.jar>` or `--addclasspath <path/to/jar.jar>` to load additional classes.

{% highlight bash %}
bin/start-scala-shell.sh [local | remote <host> <port> | yarn] --addclasspath <path/to/jar.jar>
{% endhighlight %}


## Setup

To get an overview of what options the Scala Shell provides, please use

{% highlight bash %}
bin/start-scala-shell.sh --help
{% endhighlight %}

### Local

To use the shell with an integrated Flink cluster just execute:

{% highlight bash %}
bin/start-scala-shell.sh local
{% endhighlight %}


### Remote

To use it with a running cluster start the scala shell with the keyword `remote`
and supply the host and port of the JobManager with:

{% highlight bash %}
bin/start-scala-shell.sh remote <hostname> <portnumber>
{% endhighlight %}

### Yarn Scala Shell cluster

The shell can deploy a Flink cluster to YARN, which is used exclusively by the
shell. The number of YARN containers can be controlled by the parameter `-n <arg>`.
The shell deploys a new Flink cluster on YARN and connects the
cluster. You can also specify options for YARN cluster such as memory for
JobManager, name of YARN application, etc.

For example, to start a Yarn cluster for the Scala Shell with two TaskManagers
use the following:

{% highlight bash %}
 bin/start-scala-shell.sh yarn -n 2
{% endhighlight %}

For all other options, see the full reference at the bottom.


### Yarn Session

If you have previously deployed a Flink cluster using the Flink Yarn Session,
the Scala shell can connect with it using the following command:

{% highlight bash %}
 bin/start-scala-shell.sh yarn
{% endhighlight %}


## Full Reference

{% highlight bash %}
Flink Scala Shell
Usage: start-scala-shell.sh [local|remote|yarn] [options] <args>...

Command: local [options]
Starts Flink scala shell with a local Flink cluster
  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
Command: remote [options] <host> <port>
Starts Flink scala shell connecting to a remote cluster
  <host>
        Remote host name as string
  <port>
        Remote port as integer

  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
Command: yarn [options]
Starts Flink scala shell connecting to a yarn cluster
  -n arg | --container arg
        Number of YARN container to allocate (= Number of TaskManagers)
  -jm arg | --jobManagerMemory arg
        Memory for JobManager container [in MB]
  -nm <value> | --name <value>
        Set a custom name for the application on YARN
  -qu <arg> | --queue <arg>
        Specifies YARN queue
  -s <arg> | --slots <arg>
        Number of slots per TaskManager
  -tm <arg> | --taskManagerMemory <arg>
        Memory per TaskManager container [in MB]
  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
  --configDir <value>
        The configuration directory.
  -h | --help
        Prints this usage text
{% endhighlight %}

{% top %}