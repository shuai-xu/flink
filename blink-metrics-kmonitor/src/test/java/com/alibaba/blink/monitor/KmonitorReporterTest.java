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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.core.MetricsTags;
import com.taobao.kmonitor.impl.KMonitorConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test case for {@link KmonitorReporter}.
 */
public class KmonitorReporterTest {

	private static final int MAX_TAG_VALUE_LENGTH = 20;

	private static final String TEST_QUEUE = "test_queue";
	private static final String TEST_CLUSTER_NAME = "test_session_name";

	private Gauge<String> stringGauge;
	private Gauge<Float> floatGauge;
	private Counter counter;
	private Histogram histogram;
	private Meter meter;
	private TestJobFlinkLatency jobFlinkLatency;

	HistogramStatistics histogramStatistics;
	HistogramStatistics flinkLatencyStatistics;

	public KmonitorReporterTest() {
		// create metrics
		stringGauge = new TestStringGauge();
		floatGauge = new TestFloatGauge();
		counter = new TestCounter();
		histogram = new TestHistogram();
		meter = new TestMeter();
		jobFlinkLatency = new TestJobFlinkLatency(100);

		histogramStatistics = histogram.getStatistics();
		flinkLatencyStatistics = jobFlinkLatency.getStatistics();
	}

	@Test
	public void testConfigTenantName() {
		KmonitorReporter reporter = spy(new KmonitorReporter());
		doNothing().when(reporter).initKMonitor();
		doNothing().when(reporter).startKMonitor();

		MetricConfig config = new MetricConfig();
		config.setProperty(KmonitorReporter.CONF_TENANT_NAME, "test_tenant_name");

		reporter.open(config);

		Assert.assertEquals("test_tenant_name", KMonitorConfig.getKMonitorTenantName());
	}

	@Test
	public void testFilterCharacters() {
		KmonitorReporter reporter = spy(new KmonitorReporter());

		Assert.assertEquals("1234567890123456", reporter.filterCharacters("1234567890123456"));
		Assert.assertEquals("123456789012345", reporter.filterCharacters("1234567890123456", 15));
		Assert.assertEquals("1234567890123456_", reporter.filterCharacters("1234567890123456("));
	}

	@Test
	public void testJobMetrics() {
		String[][] scopes = {
			{ScopeFormat.SCOPE_HOST, "test_host"},
			{ScopeFormat.SCOPE_TASKMANAGER_ID, "test_manager_id"},
			{ScopeFormat.SCOPE_JOB_ID, "test_job_id"},
			{ScopeFormat.SCOPE_JOB_NAME, "test_job_name"},
			{ScopeFormat.SCOPE_TASK_VERTEX_ID, "test_vertex_id"},
			{ScopeFormat.SCOPE_TASK_ATTEMPT_ID, "1"},
			{ScopeFormat.SCOPE_TASK_NAME, "test_task_name"},
			{ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, "3"},
			{ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, "1"}
		};

		Object[][] expectedMetrics = new Object[][] {
			{"test_job_name.counter", new Double(counter.getCount())},
			{"test_job_name.histogram." + KmonitorReporter.HISTOGRAM_COUNT_SUFFIX, new Double(histogram.getCount())},
			{"test_job_name.histogram." + KmonitorReporter.HISTOGRAM_MAX_SUFFIX, new Double(histogramStatistics.getMax())},
			{"test_job_name.histogram." + KmonitorReporter.HISTOGRAM_MEAN_SUFFIX, new Double(histogramStatistics.getMean())},
			{"test_job_name.histogram." + KmonitorReporter.HISTOGRAM_MIN_SUFFIX, new Double(histogramStatistics.getMin())},
			{"test_job_name." + KmonitorReporter.FLINK_METRIC_LATENCY, new Double(49.0)},
			{"test_job_name.meter", new Double(meter.getRate())},
			{"test_job_name.meter." + KmonitorReporter.METER_COUNT_SUFFIX, new Double(meter.getCount())},
			{"test_job_name.meter." + KmonitorReporter.METER_RATE_SUFFIX, new Double(meter.getRate())},
			{"test_job_name.test_group.float_gauge", new Double(floatGauge.getValue())}
		};

		testBase(scopes, expectedMetrics, "test_job_name");
	}

	@Test
	public void testClusterMetrics() {
		String[][] scopes = {
			{ScopeFormat.SCOPE_HOST, "test_host"}
		};

		Object[][] expectedMetrics = new Object[][] {
			{TEST_CLUSTER_NAME + ".counter", new Double(counter.getCount())},
			{TEST_CLUSTER_NAME + ".histogram." + KmonitorReporter.HISTOGRAM_COUNT_SUFFIX, new Double(histogram.getCount())},
			{TEST_CLUSTER_NAME + ".histogram." + KmonitorReporter.HISTOGRAM_MAX_SUFFIX, new Double(histogramStatistics.getMax())},
			{TEST_CLUSTER_NAME + ".histogram." + KmonitorReporter.HISTOGRAM_MEAN_SUFFIX, new Double(histogramStatistics.getMean())},
			{TEST_CLUSTER_NAME + ".histogram." + KmonitorReporter.HISTOGRAM_MIN_SUFFIX, new Double(histogramStatistics.getMin())},
			{TEST_CLUSTER_NAME + ".latency." + KmonitorReporter.HISTOGRAM_COUNT_SUFFIX, new Double(jobFlinkLatency.getCount())},
			{TEST_CLUSTER_NAME + ".latency." + KmonitorReporter.HISTOGRAM_MAX_SUFFIX, new Double(flinkLatencyStatistics.getMax())},
			{TEST_CLUSTER_NAME + ".latency." + KmonitorReporter.HISTOGRAM_MEAN_SUFFIX, new Double(flinkLatencyStatistics.getMean())},
			{TEST_CLUSTER_NAME + ".latency." + KmonitorReporter.HISTOGRAM_MIN_SUFFIX, new Double(flinkLatencyStatistics.getMin())},
			{TEST_CLUSTER_NAME + ".meter", new Double(meter.getRate())},
			{TEST_CLUSTER_NAME + ".meter." + KmonitorReporter.METER_COUNT_SUFFIX, new Double(meter.getCount())},
			{TEST_CLUSTER_NAME + ".meter." + KmonitorReporter.METER_RATE_SUFFIX, new Double(meter.getRate())},
			{TEST_CLUSTER_NAME + ".test_group.float_gauge", new Double(floatGauge.getValue())}
		};

		testBase(scopes, expectedMetrics, TEST_CLUSTER_NAME);
	}

	private void testBase(final String[][] scopes, final Object[][] expectedMetrics, final String expectedKmonServiceName) {
		// setup KmonitorReporter
		KMonitor kMonitor = spy(KMonitorFactory.getKMonitor(KmonitorReporter.KMONITOR_GROUP_NAME));

		SendAnswer sendAnswer = new SendAnswer();
		doAnswer(sendAnswer).when(kMonitor).report(anyString(), any(MetricsTags.class), anyFloat());

		KmonitorReporter reporter = spy(new KmonitorReporter());
		Whitebox.setInternalState(reporter, "kMonitor", kMonitor);

		Whitebox.setInternalState(reporter, "maxTagValueLength", new Integer(MAX_TAG_VALUE_LENGTH));

		Whitebox.setInternalState(reporter, "latencyAggType", "min");

		Map<String, String> globalTagsMap = (Map<String, String>) Whitebox.getInternalState(reporter, "globalTagsMap");
		globalTagsMap.put(KmonitorReporter.TAG_QUEUE, TEST_QUEUE);
		globalTagsMap.put(KmonitorReporter.TAG_CLUSTER_NAME, TEST_CLUSTER_NAME);

		Whitebox.setInternalState(reporter, "flinkClusterName", TEST_CLUSTER_NAME);

		// setup MetricGroup
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
			QueryScopeInfo queryScopeInfo = new QueryScopeInfo.TaskQueryScopeInfo("", "" , 0, "");
			when(metricGroup.getQueryServiceMetricInfo(Mockito.any(CharacterFilter.class))).thenReturn(queryScopeInfo);

			when(frontMetricGroup.getAllVariables()).thenReturn(scopeVariables);
			when(frontMetricGroup.getScopeComponents()).thenReturn(scopeComponents);
			when(frontMetricGroup.getReference()).thenReturn(metricGroup);
		}

		FrontMetricGroup frontMetricGroupWithName = mock(FrontMetricGroup.class);
		AbstractMetricGroup metricGroupWithName = mock(AbstractMetricGroup.class);
		{
			QueryScopeInfo queryScopeInfo = new QueryScopeInfo.TaskQueryScopeInfo("", "" , 0, "test_group");
			when(metricGroupWithName.getQueryServiceMetricInfo(Mockito.any(CharacterFilter.class))).thenReturn(queryScopeInfo);

			when(frontMetricGroupWithName.getAllVariables()).thenReturn(scopeVariables);
			when(frontMetricGroupWithName.getScopeComponents()).thenReturn(scopeComponents);
			when(frontMetricGroupWithName.getReference()).thenReturn(metricGroupWithName);
		}

		// test adding metrics
		reporter.notifyOfAddedMetric(stringGauge, "string_gauge", frontMetricGroup);
		reporter.notifyOfAddedMetric(floatGauge, "float_gauge", frontMetricGroupWithName);
		reporter.notifyOfAddedMetric(counter, "counter", frontMetricGroup);
		reporter.notifyOfAddedMetric(histogram, "histogram", frontMetricGroup);
		reporter.notifyOfAddedMetric(meter, "meter", frontMetricGroup);
		reporter.notifyOfAddedMetric(jobFlinkLatency, KmonitorReporter.FLINK_METRIC_LATENCY, frontMetricGroup);

		{
			// report metrics
			reporter.report();

			// check the size of all reported metrics
			List<Object[]> answerList = sendAnswer.getRecords();
			Assert.assertEquals(expectedMetrics.length, sendAnswer.getSize());

			Collections.sort(answerList, (o1, o2) -> o1[0].toString().compareTo(o2[0].toString()));

			for (int i = 0; i < answerList.size(); i++) {
				// check metrics
				Object[] answer = answerList.get(i);
				Assert.assertEquals(expectedMetrics[i][0], answer[0]);
				Assert.assertNotNull("the failed metricName is " + expectedMetrics[i][0], answer[1]);
				Assert.assertEquals("the failed metricName is " + expectedMetrics[i][0], expectedMetrics[i][1], answer[2]);

				// check the size of tags
				Map<String, String> tagsMap = ((ImmutableMetricTags) answer[1]).getUnmodifiableTags();
				Assert.assertEquals(scopeVariables.size() + 3 /*queue, session_name, kmon_service_name*/, tagsMap.size());

				// check tags from scope variables
				for (Map.Entry<String, String> entry : scopeVariables.entrySet()) {
					String expectedKey = entry.getKey();
					expectedKey = expectedKey.substring(1, expectedKey.length() - 1);

					String expectedValue = entry.getValue();
					expectedValue = expectedValue.substring(0,
						expectedValue.length() < MAX_TAG_VALUE_LENGTH ? expectedValue.length() : MAX_TAG_VALUE_LENGTH);

					Assert.assertEquals("the tagName is " + expectedKey, expectedValue, tagsMap.get(expectedKey));
				}

				// check global tags
				String expectedKey = KmonitorReporter.TAG_KMON_SERVICE_NAME;
				Assert.assertEquals("the tagName is " + expectedKey, expectedKmonServiceName, tagsMap.get(expectedKey));

				expectedKey = KmonitorReporter.TAG_QUEUE;
				Assert.assertEquals("the tagName is " + expectedKey, "test_queue", tagsMap.get(expectedKey));

				expectedKey = KmonitorReporter.TAG_CLUSTER_NAME;
				Assert.assertEquals("the tagName is " + expectedKey, "test_session_name", tagsMap.get(expectedKey));
			}
		}

		// test removing metric
		reporter.notifyOfRemovedMetric(meter, "meter", frontMetricGroup);

		{
			sendAnswer.clear();
			reporter.report();

			// check the size of all metrics
			Assert.assertEquals(expectedMetrics.length - 3, sendAnswer.getSize());
		}
	}

	static class TestCounter implements Counter {
		@Override
		public void inc() {

		}

		@Override
		public void inc(long n) {

		}

		@Override
		public void dec() {

		}

		@Override
		public void dec(long n) {

		}

		@Override
		public long getCount() {
			return 1;
		}
	}

	static class TestStringGauge implements Gauge<String> {
		@Override
		public String getValue() {
			return "abcde";
		}
	}

	static class TestFloatGauge implements Gauge<Float> {
		@Override
		public Float getValue() {
			return 1.0f;
		}
	}

	static class TestJobFlinkLatency extends DescriptiveStatisticsHistogram {

		public TestJobFlinkLatency(int windowSize) {
			super(windowSize);
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new HistogramStatistics() {
				@Override
				public double getQuantile(double quantile) {
					return 50;
				}

				@Override
				public long[] getValues() {
					return new long[0];
				}

				@Override
				public int size() {
					return 0;
				}

				@Override
				public double getMean() {
					return 0;
				}

				@Override
				public double getStdDev() {
					return 0;
				}

				@Override
				public long getMax() {
					return 999;
				}

				@Override
				public long getMin() {
					return 49;
				}
			};
		}
	}

	static class TestMeter implements Meter {
		@Override
		public void markEvent() {

		}

		@Override
		public void markEvent(long n) {

		}

		@Override
		public double getRate() {
			return 0;
		}

		@Override
		public long getCount() {
			return 2;
		}
	}

	static class TestHistogram implements Histogram {
		@Override
		public void update(long value) {

		}

		@Override
		public long getCount() {
			return 0;
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new HistogramStatistics() {
				@Override
				public double getQuantile(double quantile) {
					return 3;
				}

				@Override
				public long[] getValues() {
					return new long[0];
				}

				@Override
				public int size() {
					return 0;
				}

				@Override
				public double getMean() {
					return 0;
				}

				@Override
				public double getStdDev() {
					return 0;
				}

				@Override
				public long getMax() {
					return 999;
				}

				@Override
				public long getMin() {
					return 111;
				}
			};
		}
	}

	static class SendAnswer implements Answer {

		private List<Object[]> records = new ArrayList<Object[]>();

		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {
			records.add(invocation.getArguments());

			return null;
		}

		public void clear() {
			records.clear();
		}

		public int getSize() {
			return records.size();
		}

		public List<Object[]> getRecords() {
			return records;
		}
	}

}
