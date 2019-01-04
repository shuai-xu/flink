---
title: "SQL Resource"
nav-parent_id: tableapi
nav-pos: 100
is_beta: true
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

   Flink’s Table & SQL API make it easily to describe a job, while resource setting is not clearly as
the execution details is invisible through sql interface. 
   We provide two granularity to set resource, and describe them according to batch and streaming scenarios.

* This will be replaced by the TOC
{:toc}

oarse-grained
---------------

### Streaming job

We can set all resource according to configuration.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every node, StreamExecutionEnvironment::getParallelism(its default value is cpu cores num of the client host) is default</td>
   </tr>
   <tr>
      <td><strong>sql.exec.source.parallelism</strong></td>
      <td>Parallelism of every source node, if it is not set, use sql.resource.default.parallelism</td>
   </tr>
   <tr>
      <td><strong>sql.exec.sink.parallelism</strong></td>
      <td>Parallelism of every sink node, if it is not set, use sql.resource.default.parallelism</td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.memory.mb</strong></td>
      <td>Basic heap memory used by a task, default is 64MB</td>
   </tr>
   <tr>
      <td><strong>sql.resource.source.default.memory.mb</strong></td>
      <td>Heap memory used by a source task, default is 128MB</td>
   </tr>
   <tr>
      <td><strong>sql.resource.sink.default.memory.mb</strong></td>
      <td>Heap memory used by a sink task, default is 128MB</td>
   </tr>
  </tbody>
</table>

{% top %}

### batch job

This section describes how to set resource with config. There are three ways to work:

* set all resource according to configuration

* only infer source parallelism according to source data size or source rowCount (suitable for Csv, Parquet and sources)

* infer all resource according to full table stats.

#### set all resource according to configuration

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set NONE，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every node, StreamExecutionEnvironment::getParallelism(its default value is cpu cores num of the client host) is default</td>
   </tr>
  </tbody>
</table>


The following configuration can work by default, and generally does not need to be adjusted：


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by a task, default is 0.3
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.memory.mb</strong></td>
      <td>Basic heap memory used by a task, default is 64MB</td>
   </tr>
   
   <tr>
         <td><strong>sql.resource.source.default.memory.mb</strong></td>
         <td>Heap memory used by a source task, default is 128MB</td>
    </tr>
      
   <tr>
         <td><strong>sql.resource.sink.default.memory.mb</strong></td>
         <td>Heap memory used by a sink task, default is 128MB</td>
   </tr>
         
   <tr>
         <td><strong>sql.resource.hash-agg.table.memory.mb</strong></td>
         <td>Managed memory used by a hashAgg task, default is 128MB</td>
   </tr>
   
   <tr>
         <td><strong>sql.resource.hash-join.table.memory.mb</strong></td>
         <td>Managed memory used by a hashJoin task，default is 512MB</td>
   </tr>
         
   <tr>
         <td><strong>sql.resource.sort.buffer.memory.mb</strong></td>
         <td>Managed memory used by a sort task，default is 256MB</td>
   </tr>
            
   <tr>
        <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
        <td>Managed memory used by a external-buffer task, default is 10MB</td>
   </tr>
  </tbody>
</table>


#### only infer source parallelism according to source data size

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set ONLY_SOURCE，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.rows-per-partition</strong></td>
         <td>
              How many records one source task process, default is 100w
         </td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.mb-per-partition</strong></td>
       <td>How much data one source task process, default is Integer.Max_value</td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.parallelism.max</strong></td>
       <td>Max parallelism the source node should be, default is 1000</td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.parallelism</strong></td>
      <td>Parallelism of every operator, source excluded. StreamExecutionEnvironment::getParallelism(its default value is cpu cores num of the client host) is default</td>
   </tr>
      
  </tbody>
</table>


The following configuration can work by default, and generally does not need to be adjusted：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by a task, default is 0.3
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.default.memory.mb</strong></td>
      <td>Basic heap memory used by a task, default is 64MB</td>
   </tr>
   
   <tr>
         <td><strong>sql.resource.source.default.memory.mb</strong></td>
         <td>Heap memory used by a source task, default is 128MB</td>
    </tr>
      
   <tr>
         <td><strong>sql.resource.sink.default.memory.mb</strong></td>
         <td>Heap memory used by a sink task, default is 128MB</td>
   </tr>
         
   <tr>
         <td><strong>sql.resource.hash-agg.table.memory.mb</strong></td>
         <td>Managed memory used by a hashAgg task, default is 128MB</td>
   </tr>
   
   <tr>
         <td><strong>sql.resource.hash-join.table.memory.mb</strong></td>
         <td>Managed memory used by a hashJoin task，default is 512MB</td>
   </tr>
         
   <tr>
         <td><strong>sql.resource.sort.buffer.memory.mb</strong></td>
         <td>Managed memory used by a sort task，default is 256MB</td>
   </tr>
            
   <tr>
        <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
        <td>Managed memory used by a external-buffer task, default is 10MB</td>
   </tr>
  </tbody>
</table>

#### infer all resource according to full table stats：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.infer.mode</strong></td>
      <td>
        Set ALL，NONE is default
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.rows-per-partition</strong></td>
         <td>
              How many records one task process, default is 100w
         </td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.mb-per-partition</strong></td>
       <td>How much data one source task process, default is Interger.Max_value</td>
   </tr>
   <tr>
       <td><strong>sql.resource.infer.source.parallelism.max</strong></td>
       <td>Max parallelism the source node should be, default is 1000</td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.operator.parallelism.max</strong></td>
      <td>Max parallelism the operator node should be, default is 800</td>
   </tr>
   <tr>
      <td><strong>sql.resource.infer.operator.memory.max.mb</strong></td>
      <td>Max managed memory the operator node can use, default is 1024MB</td>
   </tr>
      
  </tbody>
</table>

The following configuration can work by default, and generally does not need to be adjusted：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Config name</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>sql.resource.default.cpu</strong></td>
      <td>
        Cpu used by a task, default is 0.3
      </td>
   </tr>
   <tr>
      <td><strong>sql.resource.external-buffer.memory.mb</strong></td>
      <td>Managed memory use by a external-buffer task, default is 10MB</td>
   </tr>
  </tbody>
</table>
{% top %}





