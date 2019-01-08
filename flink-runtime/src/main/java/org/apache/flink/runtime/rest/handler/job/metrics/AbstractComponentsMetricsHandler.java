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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentsFilterParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentsMetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Request handler that returns for tasks a list of all available metrics or the values for a set of metrics.
 */

public abstract class AbstractComponentsMetricsHandler<M extends MessageParameters> extends
	AbstractRestHandler<RestfulGateway, EmptyRequestBody, ComponentsMetricCollectionResponseBody, M> {

	private final MetricFetcher metricFetcher;

	public AbstractComponentsMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody, ComponentsMetricCollectionResponseBody, M> messageHeaders,
			MetricFetcher metricFetcher) {
		super(localRestAddress, leaderRetriever, timeout, headers, messageHeaders);
		this.metricFetcher = requireNonNull(metricFetcher, "metricFetcher must not be null");
	}

	@Override
	protected final CompletableFuture<ComponentsMetricCollectionResponseBody> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, M> request,
			@Nonnull RestfulGateway gateway) throws RestHandlerException {
		metricFetcher.update();

		final Set<String> requestedComponenets = getAvailableComponents(
			new HashSet<>(request.getQueryParameter(ComponentsFilterParameter.class)),
			gateway, request);
		if (requestedComponenets == null || requestedComponenets.isEmpty()) {
			return CompletableFuture.completedFuture(
				new ComponentsMetricCollectionResponseBody(Collections.emptyList()));
		} else {
			final Map<String, MetricStore.ComponentMetricStore> componentId2MetricStores = getComponentId2MetricStores(
				request,
				metricFetcher.getMetricStore(),
				requestedComponenets);

			if (componentId2MetricStores == null) {
				return CompletableFuture.completedFuture(
					new ComponentsMetricCollectionResponseBody(Collections.emptyList()));
			} else {
				final Set<String> requestedMetrics = new HashSet<>(request.getQueryParameter(
					MetricsFilterParameter.class));
				List<ComponentMetric> componentMetricList = new ArrayList<>();
				for (Map.Entry<String, MetricStore.ComponentMetricStore> componentId2MetricStore : componentId2MetricStores.entrySet()) {
					MetricStore.ComponentMetricStore componentMetricStore = componentId2MetricStore.getValue();
					String componentId = componentId2MetricStore.getKey();
					List<Metric> metrics;
					if (componentMetricStore.metrics == null) {
						continue;
					} else if (requestedMetrics.isEmpty()) {
						metrics = getAvailableMetrics(componentMetricStore);
					} else {
						metrics = getRequestedMetrics(componentMetricStore, requestedMetrics);
					}
					//todo add timestamp
					ComponentMetric componentMetric = new ComponentMetric(componentId, -1L, metrics);
					componentMetricList.add(componentMetric);
				}
				return CompletableFuture.completedFuture(new ComponentsMetricCollectionResponseBody(componentMetricList));
			}
		}
	}

	/**
	 * Returns the {@link MetricStore.ComponentMetricStore} that should be queried for metrics.
	 */
	@Nullable
	protected abstract Map<String, MetricStore.ComponentMetricStore> getComponentId2MetricStores(
		HandlerRequest<EmptyRequestBody, M> request,
		MetricStore metricStore,
		Set<String> requestedComponents);

	protected Set<String> getAvailableComponents(Set<String> requestedComponenets, RestfulGateway gateway, HandlerRequest<EmptyRequestBody, M> request) throws NotFoundException {
		return requestedComponenets;
	}

	private static List<Metric> getAvailableMetrics(MetricStore.ComponentMetricStore componentMetricStore) {
		return componentMetricStore.metrics
			.keySet()
			.stream()
			.map(Metric::new)
			.collect(Collectors.toList());
	}

	private static List<Metric> getRequestedMetrics(
			MetricStore.ComponentMetricStore componentMetricStore,
			Set<String> requestedMetrics) throws RestHandlerException {

		final List<Metric> metrics = new ArrayList<>(requestedMetrics.size());
		for (final String requestedMetric : requestedMetrics) {
			final String value = componentMetricStore.getMetric(requestedMetric, null);
			if (value != null) {
				metrics.add(new Metric(requestedMetric, value));
			}
		}
		return metrics;
	}

}
