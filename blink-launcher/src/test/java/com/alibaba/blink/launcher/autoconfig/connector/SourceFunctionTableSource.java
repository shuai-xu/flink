/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.launcher.autoconfig.connector;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;

import java.io.Serializable;
import java.util.List;

import scala.Option;

/**
 * A simplified class, mostly copied from blink-connector, for IT test only.
 */
public class SourceFunctionTableSource<OUT> implements BatchTableSource<OUT>, StreamTableSource<OUT>, Serializable {
	private static final long serialVersionUID = 7078000231877526561L;
	protected TypeInformation<OUT> returnTypeInfo;
	private transient DataStream dataStream;
	private transient DataStreamSource boundedStreamSource;
	private SourceFunction<OUT> sourceFunction;

	public SourceFunctionTableSource() {}

	public SourceFunctionTableSource(SourceFunction<OUT> sourceFunction) {
		this.sourceFunction = sourceFunction;
	}

	public SourceFunction<OUT> getSourceFunction()  {
		if (this.sourceFunction != null) {
			return this.sourceFunction;
		} else {
			throw new UnsupportedOperationException("SourceFunction has not been setup yet");
		}
	}

	public String explainSource() {
		return this.getSourceFunction().toString();
	}

	public ResourceSpec getResource() {
		return null;
	}

	public DataStream<OUT> getDataStream(StreamExecutionEnvironment execEnv) {
		if (this.dataStream == null) {
			SourceFunction<OUT> sourceFunction = this.getSourceFunction();
			if (sourceFunction instanceof AbstractParallelSource) {
				((AbstractParallelSource) sourceFunction).disableWatermarkEmitter();
			}

			this.dataStream = execEnv.addSource(sourceFunction).name(this.explainSource());
		}

		return this.dataStream;
	}

	public DataStream<OUT> getBoundedStream(StreamExecutionEnvironment execEnv) {
		if (this.boundedStreamSource == null) {
			SourceFunction<OUT> sourceFunction = this.getSourceFunction();
			TypeInformation typeInfo;
			if (sourceFunction instanceof ResultTypeQueryable) {
				typeInfo = ((ResultTypeQueryable) sourceFunction).getProducedType();
			} else {
				try {
					typeInfo = TypeExtractor.createTypeInfo(SourceFunction.class,
						sourceFunction.getClass(),
						0,
						(TypeInformation) null,
						(TypeInformation) null);
				} catch (InvalidTypesException var7) {
					throw new RuntimeException("Fail to get type information of source function", var7);
				}
			}
			this.boundedStreamSource = execEnv.addSource(sourceFunction,
				String.format("%s-%s", this.explainSource(), "Batch"), typeInfo);
			int parallelism = -1;
			if (sourceFunction instanceof AbstractParallelSource) {
				AbstractParallelSource parallelSource = (AbstractParallelSource) sourceFunction;
				parallelSource.disableParallelRead();

				try {
					List<String> partitionList = parallelSource.getPartitionList();
					if (partitionList != null) {
						parallelism = partitionList.size();
					}
				} catch (Exception var6) {
				}
			}
			if (parallelism > 0) {
				this.boundedStreamSource.setParallelism(parallelism);
				this.boundedStreamSource.getTransformation().setParallelismLocked(true);
			}
			ResourceSpec resource = this.getResource();
			if (resource != null) {
				this.boundedStreamSource.getTransformation().setResources(resource, resource);
			}
		}

		return this.boundedStreamSource;
	}

	public TableStats getTableStats() {
		return null;
	}

	public DataType getReturnType() {
		return DataTypes.of(this.getProducedType());
	}

	private TypeInformation<OUT> getProducedType() {
		if (this.returnTypeInfo != null) {
			return this.returnTypeInfo;
		} else {
			SourceFunction<OUT> sourceFunction = this.getSourceFunction();
			if (sourceFunction instanceof ResultTypeQueryable) {
				this.returnTypeInfo = ((ResultTypeQueryable) sourceFunction).getProducedType();
			} else {
				try {
					this.returnTypeInfo = TypeExtractor.createTypeInfo(SourceFunction.class,
						sourceFunction.getClass(),
						0,
						(TypeInformation) null,
						(TypeInformation) null);
				} catch (InvalidTypesException var3) {
					throw new RuntimeException("Fail to get type information of source function", var3);
				}
			}

			return this.returnTypeInfo;
		}
	}

	public TableSchema getTableSchema() {
		return TableSchema.fromDataType(getReturnType(), Option.empty());
	}
}
