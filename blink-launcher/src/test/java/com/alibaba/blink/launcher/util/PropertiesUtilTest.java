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

package com.alibaba.blink.launcher.util;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Test loading properties with trimmed value.
 */
public class PropertiesUtilTest extends AbstractTestBase {

	@Test
	public void testPropertiesWithWhiteSpace() throws IOException, URISyntaxException {
		String properties = "key=value\nkey0=\n=value0\n key1= value1\nkey2 =value2 \n key3 = value3 \n\tkey4\t=\tvalue4\t\nkey5=\t\t\t   value5     \t";
		String tmpFilePath = createTempFile("test_trim.properties", properties);
		InputStream inputStream = new FileInputStream(new URI(tmpFilePath).getPath());
		Properties prop = PropertiesUtil.loadWithTrimmedValues(inputStream);

		Assert.assertNotNull(prop);
		Assert.assertEquals(prop.getProperty("key"), "value");
		Assert.assertEquals(prop.getProperty("key0"), "");
		Assert.assertEquals(prop.getProperty(""), "value0");
		Assert.assertEquals(prop.getProperty("key1"), "value1");
		Assert.assertEquals(prop.getProperty("key2"), "value2");
		Assert.assertEquals(prop.getProperty("key3"), "value3");
		Assert.assertEquals(prop.getProperty("key4"), "value4");
		Assert.assertEquals(prop.getProperty("key5"), "value5");
	}

	@Test
	public void testEmptyProperties() throws IOException, URISyntaxException {
		String properties = "";
		String tmpFilePath = createTempFile("test_empty.properties", properties);
		InputStream inputStream = new FileInputStream(new URI(tmpFilePath).getPath());
		Properties prop = PropertiesUtil.loadWithTrimmedValues(inputStream);

		Assert.assertNotNull(prop);
	}

	@Test
	public void testEmptyKV() throws IOException, URISyntaxException {
		String properties = "=\n   = \t   ";
		String tmpFilePath = createTempFile("test_empty_kv.properties", properties);
		InputStream inputStream = new FileInputStream(new URI(tmpFilePath).getPath());
		Properties prop = PropertiesUtil.loadWithTrimmedValues(inputStream);

		Assert.assertNotNull(prop);
		Assert.assertEquals(prop.getProperty(""), "");
	}
}
