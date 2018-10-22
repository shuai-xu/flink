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

package com.alibaba.blink.launcher.api;

import org.apache.flink.table.api.TableEnvironment;

import java.util.Properties;

/**
 * The interface of TableAPI job builder.
 *
 * <p>Example:
 *
 * {@code
 *
 * public class TableApiDemo implements JobBuilder{
 *
 * 	@Override
 * 	public void build(TableEnvironment tEnv, Properties properties) {
 *
 * 	    // define source schema
 * 		RichTableSchema randomSourceSchema = new RichTableSchema(
 * 				new String[] {"key", "c1", "c2", "c3"},
 * 				new TypeInformation[] {
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO});
 *
 *      // define sink schema
 * 		RichTableSchema printSinkSchema = new RichTableSchema(
 * 				new String[] {"key", "c1", "c2", "c3"},
 * 				new TypeInformation[] {
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO,
 * 						BasicTypeInfo.STRING_TYPE_INFO});
 *
 * 		// create printSink
 * 	    TableFactory printTableFactory = new PrintTableFactory();
 * 		TableSink printTableSink =
 * 	    	printTableFactory.createTableSink("print", printSinkSchema, new TableProperties());
 *
 * 	    // register table source
 * 		tEnv.registerTableSource("source", new RandomTableSource("soruce",randomSourceSchema));
 *
 * 	    // register aggregate function
 * 		tEnv.registerFunction("concat_agg", new ConcatAggFunction());
 *
 * 	    // obtain a table by scan a registered table
 * 		Table tab = tEnv.scan("source");
 *
 * 	    // query logic
 * 		Table result = tab.groupBy("key").select("key, concat_agg(c1)");
 *
 * 	    // write the result to sink table.
 * 		result.writeToSink(printTableSink);
 * 	}
 * }
 * }
 */
public interface JobBuilder {

	/**
	 * The main interface of TableAPI job builder.
	 *
	 * @param env A TableEnvironment can be used to:
	 *  - register a [[TableSource]] as a table in the catalog
	 *  - scan a registered table to obtain a [[Table]]
	 *  - register a [[ScalarFunction]] in the catalog
	 *  - register a [[TableFunction]] in the catalog
	 *  - register a [[AggregateFunction]] in the catalog
	 *
	 * @param properties the job configs
	 */
	void build(TableEnvironment env, Properties properties);
}
