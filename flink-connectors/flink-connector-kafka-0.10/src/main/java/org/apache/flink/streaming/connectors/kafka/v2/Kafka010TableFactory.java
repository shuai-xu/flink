/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.v2.common.TableBaseInfo;
import org.apache.flink.streaming.connectors.kafka.v2.common.util.DateUtil;
import org.apache.flink.streaming.connectors.kafka.v2.common.util.SourceUtils;
import org.apache.flink.streaming.connectors.kafka.v2.input.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.v2.sink.Kafka010OutputFormat;
import org.apache.flink.streaming.connectors.kafka.v2.sink.Kafka010TableSink;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.factories.BatchCompatibleTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchCompatibleStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.v2.common.util.SourceUtils.toRowTypeInfo;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/** Kafka010 TableFactory. */
public class Kafka010TableFactory extends KafkaBaseTableFactory implements
	StreamTableSourceFactory<GenericRow>,
	StreamTableSinkFactory<Tuple2<Boolean, Row>>,
	BatchTableSourceFactory<GenericRow>,
	BatchCompatibleTableSinkFactory<Tuple2<Boolean, Row>> {

	private Kafka010TableSource createSource(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);
		String topicStr = properties.getString(KafkaOptions.TOPIC);
		String topicPatternStr = properties.getString(KafkaOptions.TOPIC_PATTERN);
		Properties prop = getProperties(Kafka010Options.ESSENTIAL_CONSUMER_KEYS,
										Kafka010Options.OPTIONAL_CONSUMER_KEYS,
										properties);
		Long startTimeMs = properties.getLong(KafkaOptions.START_TIME_MILLS);
		String startDateTime = properties.getString(KafkaOptions.OPTIONAL_START_TIME);
		String timeZone = properties.getString(KafkaOptions.TIME_ZONE);
		long startInMs = startTimeMs;
		if (startInMs == -1) {
			if (StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				startDateTime = formatter.format(new Date());
			}
			try {
				startInMs = DateUtil.parseDateString(KafkaOptions.DATE_FORMAT, startDateTime, timeZone);
			} catch (ParseException e) {
				throw new RuntimeException(String.format(
						"Incorrect datetime format: %s, pls use ISO-8601 " +
						"complete date plus hours, minutes and seconds format:%s",
						startDateTime,
						KafkaOptions.DATE_FORMAT), e);
			}
		}

		// TODO: support batch mode.
		boolean isBatchMode = false;
		if (!StringUtils.isNullOrWhitespaceOnly(topicStr)) {
			List<String> topics = Arrays.asList(topicStr.split(","));
			return new Kafka010TableSource(topics, null, prop, getStartupMode(properties), startInMs, isBatchMode,
					TypeConverters.toBaseRowTypeInfo(schema.getResultType(GenericRow.class)));
		} else if (!StringUtils.isNullOrWhitespaceOnly(topicPatternStr)) {
			return new Kafka010TableSource(null, topicPatternStr, prop, getStartupMode(properties), startInMs, isBatchMode,
					TypeConverters.toBaseRowTypeInfo(schema.getResultType(GenericRow.class)));
		} else {
			throw new RuntimeException("No sufficient parameters for Kafka010." +
				"topic or topic pattern needed.");
		}
	}

	private Kafka010TableSink createSink(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);
		String topic = properties.getString(KafkaOptions.TOPIC);
		Properties prop = getProperties(
				Kafka010Options.ESSENTIAL_PRODUCER_KEYS,
				Kafka010Options.OPTIONAL_PRODUCER_KEYS,
				properties);
		KafkaConverter kafkaConverter;
		String convertClassStr = properties.getString(KafkaOptions.OPTIONAL_CONVERTER_CLASS);
		if (null != convertClassStr && !convertClassStr.isEmpty()) {
			try {
				Class converterClass = Thread.currentThread()
					.getContextClassLoader().loadClass(convertClassStr);
				kafkaConverter = (KafkaConverter) converterClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("", e);
			}
		} else {
			kafkaConverter = new DefaultKafkaConverter();
		}
		if (kafkaConverter instanceof TableBaseInfo) {
			TableBaseInfo tableBaseInfo = (TableBaseInfo) kafkaConverter;
			tableBaseInfo.setHeaderFields(schema.getHeaderFields())
						.setRowTypeInfo(toRowTypeInfo(schema.getResultRowType()))
						.setPrimaryKeys(schema.getPrimaryKeys())
						.setUserParamsMap(properties.toMap());
		}
		Kafka010OutputFormat.Builder builder = new Kafka010OutputFormat.Builder();
		builder.setKafkaConverter(kafkaConverter)
			.setProperties(prop)
			.setTopic(topic)
			.setRowTypeInfo(toRowTypeInfo(schema.getResultRowType()));
		return new Kafka010TableSink(builder, schema);
	}

	@Override
	public List<String> supportedProperties() {
		return SourceUtils.mergeProperties(Kafka010Options.ESSENTIAL_CONSUMER_KEYS,
				Kafka010Options.ESSENTIAL_PRODUCER_KEYS,
				Kafka010Options.OPTIONAL_CONSUMER_KEYS,
				Kafka010Options.OPTIONAL_PRODUCER_KEYS,
				KafkaOptions.SUPPORTED_KEYS);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "KAFKA010"); // KAFKA010
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public BatchCompatibleStreamTableSink<Tuple2<Boolean, Row>> createBatchCompatibleTableSink(Map<String, String> properties) {
		return createSink(properties);
	}

	@Override
	public BatchTableSource<GenericRow> createBatchTableSource(Map<String, String> properties) {
		return createSource(properties);
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		return createSink(properties);
	}

	@Override
	public StreamTableSource<GenericRow> createStreamTableSource(Map<String, String> properties) {
		return createSource(properties);
	}
}
