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

package org.apache.flink.table.runtime.operator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.table.runtime.functions.ProcessFunctionBase;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This is used as the base class for operators that have a user-defined
 * function. This class handles the opening and closing of the user-defined functions,
 * as part of the operator life cycle.
 *
 * @param <OUT>
 *            The output type of the operator
 * @param <F>
 *            The type of the user function
 */
@PublicEvolving
public abstract class AbstractUdfStreamOperator<OUT, F extends ProcessFunctionBase<OUT>>
		extends AbstractStreamOperator<OUT> implements Triggerable<Object, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	/** The user function. */
	protected F userFunction;

	protected transient StreamRecordCollector<OUT> collector;

	protected transient ContextImpl context;

	protected transient OnTimerContextImpl onTimerContext;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = userFunction;
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	/**
	 * Gets the user function executed in this operator.
	 * @return The user function of this operator.
	 */
	public F getUserFunction() {
		return userFunction;
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);

		if (userFunction instanceof CheckpointListener) {
			((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		StreamingFunctionUtils.restoreFunctionState(context, userFunction);
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new StreamRecordCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
			getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl(timerService);
		onTimerContext = new OnTimerContextImpl(timerService);
	}

	@Override
	public void close() throws Exception {
		super.close();
		functionsClosed = true;
		userFunction.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			functionsClosed = true;
			userFunction.close();
		}
	}

	@Override
	public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		setCurrentKey(timer.getKey());

		onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
	}

	@Override
	public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		setCurrentKey(timer.getKey());

		onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
	}

	/**
	 * Context for TimeService.
	 */
	protected class ContextImpl extends ProcessFunctionBase.Context {

		private final TimerService timerService;

		ContextImpl(TimerService timerService) {
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}
	}

	/**
	 * On Timer context.
	 */
	protected class OnTimerContextImpl extends ProcessFunctionBase.OnTimerContext {

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<?, VoidNamespace> timer;

		OnTimerContextImpl(TimerService timerService) {
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
		}

		public void setTimeDomain(TimeDomain timeDomain) {
			this.timeDomain = timeDomain;
		}
	}
}
