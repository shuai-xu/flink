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

package org.apache.flink.table.validate;

import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.RowType;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConversions;

import static org.junit.Assert.assertNotNull;

/**
 * Test for ExternalFunctionCatalog.
 */
public class ExternalFunctionCatalogTest {

	private static final FlinkTypeFactory TYPE_FACTORY = new FlinkTypeFactory(new FlinkTypeSystem());
	private static final String TEST_FUNCTION = "test";
	private static final List<Expression> EXPRESSIONS = new ArrayList<>();

	private static CatalogManager catalogManager;
	private static ExternalFunctionCatalog functionCatalog;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@BeforeClass
	public static void init() {
		catalogManager = new CatalogManager();
		functionCatalog = new ExternalFunctionCatalog(catalogManager, TYPE_FACTORY);
	}

	@Test
	public void testScalarFunction() {
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(ValidScalarUDF.class.getName()));

		assertNotNull(functionCatalog.lookupFunction(TEST_FUNCTION, JavaConversions.asScalaBuffer(EXPRESSIONS).toSeq()));
	}

	@Test
	public void testTableFunction() {
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(ValidTableUDF.class.getName()));

		assertNotNull(functionCatalog.lookupFunction(TEST_FUNCTION, JavaConversions.asScalaBuffer(EXPRESSIONS).toSeq()));
	}

	@Test
	public void testAggregateFunction() {
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(ValidAggregateUDF.class.getName()));

		assertNotNull(functionCatalog.lookupFunction(TEST_FUNCTION, JavaConversions.asScalaBuffer(EXPRESSIONS).toSeq()));
	}

	@Test
	public void testScalarFunction_NoDefaultConstructor() {
		String clazzName = InvalidScalarUDF.class.getName();

		exception.expect(RuntimeException.class);
		exception.expectMessage(
			String.format("java.lang.InstantiationException : %s", clazzName));
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(clazzName));
	}

	@Test
	public void testTableFunction_NoDefaultConstructor() {
		String clazzName = InvalidTableUD.class.getName();

		exception.expect(RuntimeException.class);
		exception.expectMessage(
			String.format("java.lang.InstantiationException : %s", clazzName));
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(clazzName));
	}

	@Test
	public void testAggregateFunction_NoDefaultConstructor() {
		String clazzName = InvalidAggregateUDF.class.getName();

		exception.expect(RuntimeException.class);
		exception.expectMessage(
			String.format("java.lang.InstantiationException : %s", clazzName));
		functionCatalog.registerFunction(TEST_FUNCTION, new CatalogFunction(clazzName));
	}

	@After
	public void close() {
		functionCatalog.dropFunction(TEST_FUNCTION);
	}

	/**
	 * The scalar function.
	 */
	public static class ValidScalarUDF extends ScalarFunction {

		private int offset;

		public ValidScalarUDF() {
			this.offset = 5;
		}

		public ValidScalarUDF(Integer offset) {
			this.offset = offset;
		}

		public String eval(Integer i) {
			return String.valueOf(i + offset);
		}
	}

	/**
	 * The table function.
	 */
	public static class ValidTableUDF extends TableFunction<Row> {
		public void eval() {
		}

		@Override
		public DataType getResultType(Object[] arguments, Class[] argTypes) {
			return new RowType(DataTypes.STRING, DataTypes.LONG);
		}
	}

	/**
	 * The aggregate function.
	 */
	public static class ValidAggregateUDF extends AggregateFunction<Long, Long> {

		public ValidAggregateUDF() {
		}

		public ValidAggregateUDF(String name, Boolean flag, Integer value) {
			// do nothing
		}

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long getValue(Long accumulator) {
			return 100L;
		}

		public void accumulate(Long acc, Long value) {
			// do nothing
		}

		@Override
		public DataType getResultType() {
			return DataTypes.LONG;
		}
	}

	/**
	 * The scalar function with no default constructor.
	 */
	public static class InvalidScalarUDF extends ScalarFunction {

		private int offset;

		public InvalidScalarUDF(Integer offset) {
			this.offset = offset;
		}

		public String eval(Integer i) {
			return String.valueOf(i + offset);
		}
	}

	/**
	 * The table function with no default constructor.
	 */
	public static class InvalidTableUD extends TableFunction<Row> {
		private long extra;

		public InvalidTableUD(Long extra) {
			this.extra = extra;
		}

		public void eval() {
		}

		@Override
		public DataType getResultType(Object[] arguments, Class[] argTypes) {
			return new RowType(DataTypes.STRING, DataTypes.LONG);
		}
	}

	/**
	 * The aggregate function with no default constructor.
	 */
	public static class InvalidAggregateUDF extends AggregateFunction<Long, Long> {

		public InvalidAggregateUDF(String name) {
		}

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long getValue(Long accumulator) {
			return 100L;
		}

		public void accumulate(Long acc, Long value) {
		}

		@Override
		public DataType getResultType() {
			return DataTypes.LONG;
		}
	}
}
