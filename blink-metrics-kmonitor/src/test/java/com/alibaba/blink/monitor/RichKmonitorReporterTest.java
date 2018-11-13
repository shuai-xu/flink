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

package com.alibaba.blink.monitor;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.core.MetricsTags;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test case for {@link RichKmonitorReporter}.
 */
public class RichKmonitorReporterTest {

	private static final int MAX_TAG_VALUE_LENGTH = 20;

	private static final String TEST_CLUSTER_NAME = "test_session_name";

	@Test
	public void testFilterCharacters() throws Exception {
		RichKmonitorReporter reporter = new RichKmonitorReporter();
		Assert.assertEquals("1234567890123456", reporter.filterCharacters("1234567890123456"));
		Assert.assertEquals("123456789012345", reporter.filterCharacters("1234567890123456", 15));
		Assert.assertEquals("1234567890123456_", reporter.filterCharacters("1234567890123456("));
		Assert.assertEquals("123456789_0123456", reporter.filterCharacters("123456789.0123456"));

		Set<Character> whitelist = new HashSet<>();
		whitelist.add('.');
		whitelist.add('-');
		whitelist.add(',');
		Whitebox.setInternalState(reporter, "charWhitelist", whitelist);
		Assert.assertEquals("1234567890123456", reporter.filterCharacters("1234567890123456"));
		Assert.assertEquals("123456789012345", reporter.filterCharacters("1234567890123456", 15));
		Assert.assertEquals("1234567890123456_", reporter.filterCharacters("1234567890123456("));
		Assert.assertEquals("123456789.0123456", reporter.filterCharacters("123456789.0123456"));
	}

	@Test
	public void testUserDefinedTags() throws Exception {
		// setup KmonitorReporter
		KMonitor kMonitor = spy(KMonitorFactory.getKMonitor(KmonitorReporter.KMONITOR_GROUP_NAME));

		KmonitorReporterTest.SendAnswer sendAnswer = new KmonitorReporterTest.SendAnswer();
		doAnswer(sendAnswer).when(kMonitor).report(anyString(), any(MetricsTags.class), anyFloat());

		RichKmonitorReporter reporter = spy(new RichKmonitorReporter());
		Whitebox.setInternalState(reporter, "kMonitor", kMonitor);

		Whitebox.setInternalState(reporter, "maxTagValueLength", new Integer(MAX_TAG_VALUE_LENGTH));

		Map<String, String> globalTagsMap = (Map<String, String>) Whitebox.getInternalState(reporter, "globalTagsMap");
		globalTagsMap.put(KmonitorReporter.TAG_CLUSTER_NAME, TEST_CLUSTER_NAME);

		Whitebox.setInternalState(reporter, "flinkClusterName", TEST_CLUSTER_NAME);

		Set<Character> whitelist = new HashSet<>();
		whitelist.add('.');
		whitelist.add('-');
		whitelist.add(',');
		whitelist.add(':');
		whitelist.add('=');
		Whitebox.setInternalState(reporter, "charWhitelist", whitelist);
		Whitebox.setInternalState(reporter, "tagDelimiter", ":");
		Whitebox.setInternalState(reporter, "tagAssigner", "=");

		// setup MetricGroup
		String[][] scopes = {
			{ScopeFormat.SCOPE_HOST, "test_host"},
			{ScopeFormat.SCOPE_TASKMANAGER_ID, "test_manager_id"}
		};

		Map<String, String> scopeVariables = new HashMap<>();
		for (String[] scope : scopes) {
			scopeVariables.put(scope[0], scope[1]);
		}

		String[] scopeComponents = new String[scopes.length];
		for (int i = 0; i < scopes.length; i++) {
			scopeComponents[i] = scopes[i][1];
		}

		FrontMetricGroup frontMetricGroup = mock(FrontMetricGroup.class);
		AbstractMetricGroup metricGroup = mock(AbstractMetricGroup.class);
		{
			QueryScopeInfo queryScopeInfo = new QueryScopeInfo.TaskManagerQueryScopeInfo("", "");
			when(metricGroup.getQueryServiceMetricInfo(Mockito.any(CharacterFilter.class))).thenReturn(queryScopeInfo);

			when(frontMetricGroup.getAllVariables()).thenReturn(scopeVariables);
			when(frontMetricGroup.getScopeComponents()).thenReturn(scopeComponents);
			when(frontMetricGroup.getReference()).thenReturn(metricGroup);
		}

		// create metrics
		Gauge<Float> floatGauge = new KmonitorReporterTest.TestFloatGauge();

		Object[] expectedMetrics = new Object[] {
			TEST_CLUSTER_NAME + ".float_gauge", new Double(floatGauge.getValue()),
		};

		// test defining tags by user
		reporter.notifyOfAddedMetric(floatGauge, "float_gauge:test_tag1=123-456:test_tag2=abc+xxxxxxxxxxxxxxxxxxxxxxxxxxxxx", frontMetricGroup);

		{
			// report metrics
			reporter.report();

			// check the size of all reported metrics
			List<Object[]> answerList = sendAnswer.getRecords();
			Assert.assertEquals(1, sendAnswer.getSize());

			// check metrics
			Object[] answer = answerList.get(0);
			Assert.assertEquals(expectedMetrics[0], answer[0]);
			Assert.assertNotNull("the failed metricName is " + expectedMetrics[0], answer[1]);
			Assert.assertEquals("the failed metricName is " + expectedMetrics[0], expectedMetrics[1], answer[2]);

			// check the size of tags
			Map<String, String> tagsMap = ((ImmutableMetricTags) answer[1]).getUnmodifiableTags();
			Assert.assertEquals(scopeVariables.size() + 2 /*session_name, kmon_service_name*/ + 2 /*test_tag1, test_tag2*/, tagsMap.size());

			// check tags from scope variables
			for (Map.Entry<String, String> entry : scopeVariables.entrySet()) {
				String expectedKey = entry.getKey();
				expectedKey = expectedKey.substring(1, expectedKey.length() - 1);

				String expectedValue = entry.getValue();
				expectedValue = expectedValue.substring(0,
					expectedValue.length() < MAX_TAG_VALUE_LENGTH ? expectedValue.length() : MAX_TAG_VALUE_LENGTH);

				Assert.assertEquals("the failed tagName is " + expectedKey, expectedValue, tagsMap.get(expectedKey));
			}

			// check global tags
			String expectedKey = KmonitorReporter.TAG_KMON_SERVICE_NAME;
			Assert.assertEquals("the tagName is " + expectedKey, "test_session_name", tagsMap.get(expectedKey));

			expectedKey = KmonitorReporter.TAG_CLUSTER_NAME;
			Assert.assertEquals("the tagName is " + expectedKey, "test_session_name", tagsMap.get(expectedKey));

			// check user-defined tags
			expectedKey = "test_tag1";
			Assert.assertEquals("the tagName is " + expectedKey, "123-456", tagsMap.get(expectedKey));

			expectedKey = "test_tag2";
			Assert.assertEquals("the tagName is " + expectedKey,
				"abc_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx".substring(0, MAX_TAG_VALUE_LENGTH), tagsMap.get(expectedKey));
		}
	}
}
