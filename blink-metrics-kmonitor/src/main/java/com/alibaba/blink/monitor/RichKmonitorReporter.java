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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;

import com.taobao.kmonitor.core.MetricsTags;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link MetricReporter} that exports via KMonitor.
 * This reporter support user to define tags via metric name with the following pattern:
 *     actualMetricName:tag1=tagvalue1:tag2=tagvalue2:...
 *
 * <p>In order to enable this reporter, please place the following configuration in flink-conf.yaml file, including:
 * <ul>
 *     <li>metrics.reporters: kmonitor</li>
 *     <li>metrics.reporter.kmonitor.class: com.alibaba.blink.monitor.RichKmonitorReporter</li>
 *     <li>metrics.reporter.kmonitor.interval: 10 SECONDS</li>
 *     <li>metrics.reporter.kmonitor.tag-delimiter: : </li>
 *     <li>metrics.reporter.kmonitor.tag-assigner: = </li>
 * </ul>
 */
public class RichKmonitorReporter extends KmonitorReporter {

	private static final String TAG_DELIMITER_KEY = "tag-delimiter";
	private static final String DEFAULT_TAG_DELIMITER = ":";
	private static final String TAG_ASSIGNER_KEY = "tag-assigner";
	private static final String DEFAULT_TAG_ASSIGNER = "=";
	private static final String WHITELISTED_CHARS = "whitelisted-chars";
	private static final String DEFAULT_WHITELISTED_CHARS = ".-";

	private String tagDelimiter = DEFAULT_TAG_DELIMITER;
	private String tagAssigner = DEFAULT_TAG_ASSIGNER;
	protected Set<Character> charWhitelist = new HashSet<>();

	@Override
	public void open(MetricConfig config) {
		super.open(config);

		String whitelistedChars = config.getString(WHITELISTED_CHARS, DEFAULT_WHITELISTED_CHARS);
		final int len = whitelistedChars.length();
		for (int i = 0; i < len; i++) {
			charWhitelist.add(whitelistedChars.charAt(i));
		}

		this.tagDelimiter = config.getString(TAG_DELIMITER_KEY, DEFAULT_TAG_DELIMITER);
		if (tagDelimiter.length() != 1) {
			throw new IllegalArgumentException("tagDelimiter miss configured.");
		}
		this.tagAssigner = config.getString(TAG_ASSIGNER_KEY, DEFAULT_TAG_ASSIGNER);
		if (tagAssigner.length() != 1) {
			throw new IllegalArgumentException("tagAssigner miss configured.");
		}

		// User configured Whitelisted Chars should not contain tagDelimiter and tagAssigner
		if (charWhitelist.contains(tagDelimiter)) {
			throw new IllegalArgumentException("Whitelisted chars should not contain tag delimiter.");
		}
		if (charWhitelist.contains(tagAssigner)) {
			throw new IllegalArgumentException("Whitelisted chars should not contain tag assigner.");
		}
	}

	@Override
	protected String filterCharacters(String str, int maxLength) {
		char[] chars = null;
		final int strLen = str.length();
		int pos = 0;

		for (int i = 0; i < strLen && i < maxLength; i++) {
			final char c = str.charAt(i);
			if (!((c >= '0' && c <= '9')
				|| (c >= 'a' && c <= 'z')
				|| (c >= 'A' && c <= 'Z'))
				&& !charWhitelist.contains(c)) {
				if (chars == null) {
					chars = str.toCharArray();
				}
				chars[pos] = '_';
			} else if (chars != null) {
				chars[pos] = c;
			}
			pos++;
		}

		return chars == null ? str.substring(0, maxLength > strLen ? strLen : maxLength) : new String(chars, 0, pos);

	}

	@Override
	protected String formMetricName(Tuple2<String, MetricGroup> metricInfo) {
		String metricName = parseForMetricName(metricInfo.f0);
		return super.formMetricName(new Tuple2<>(metricName, metricInfo.f1));
	}

	@Override
	protected MetricsTags formMetricTags(MetricGroup group, String metricName) {
		MetricsTags tags = super.formMetricTags(group, metricName);

		Map<String, String> tagsInMetricName = parseForMetricTags(metricName);
		for (Map.Entry<String, String> tag : tagsInMetricName.entrySet()) {
			tags.addTag(tag.getKey(), filterCharacters(tag.getValue(), getMaxTagValueLength()));
		}

		return tags;
	}

	private String parseForMetricName(String metricName) {
		return metricName.split(tagDelimiter)[0];
	}

	private Map<String, String> parseForMetricTags(String metricName) {
		Map<String, String> tags = new HashMap<>();
		String[] splitted =  metricName.split(tagDelimiter);
		for (int i = 1; i < splitted.length; i++) {
			String[] tag = splitted[i].split(tagAssigner);
			if (tag.length == 2) {
				tags.put(tag[0], tag[1]);
			}
		}
		return tags;
	}
}
