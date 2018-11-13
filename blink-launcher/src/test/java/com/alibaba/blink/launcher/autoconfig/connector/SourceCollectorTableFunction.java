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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.util.Collector;

/**
 * A simplified class, mostly copied from blink-connector, for IT test only.
 */
public class SourceCollectorTableFunction<IN, OUT> extends TableFunction<OUT> {

	private SourceCollector<IN, OUT> sourceCollector;
	private TypeInformation<OUT> outputType;

	private transient SourceCollectorTableFunction.InternalCollector collector;

	public SourceCollectorTableFunction(
		SourceCollector<IN, OUT> sourceCollector) {
		this.sourceCollector = sourceCollector;
		if (sourceCollector instanceof ResultTypeQueryable) {
			this.outputType = (TypeInformation<OUT>)
				((ResultTypeQueryable) sourceCollector).getProducedType();

		} else {
			this.outputType = (TypeInformation<OUT>) TypeExtractor.createTypeInfo(
				TypeExtractor.getParameterType(
					SourceCollector.class, sourceCollector.getClass(), 1));
		}
	}

	@Override
	public void open(FunctionContext context) {
		sourceCollector.open(context);
		collector = new SourceCollectorTableFunction.InternalCollector();
	}

	@Override
	public void close() {
		sourceCollector.close();
	}

	public void eval(IN in) {
		sourceCollector.parseAndCollect(in, collector);
	}

	public SourceCollector<IN, OUT> getSourceCollector() {
		return sourceCollector;
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		return DataTypes.of(this.outputType);
	}

	class InternalCollector implements Collector<OUT> {
		@Override
		public void collect(OUT out) {
			SourceCollectorTableFunction.this.collect(out);
		}

		@Override
		public void close() {
		}
	}
}
