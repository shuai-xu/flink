/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

import com.alibaba.niagara.NiagaraException;
import com.alibaba.niagara.NiagaraJNI;

/**
 * Niagara DB instance, one process only has one Niagara DB.
 */
public class NiagaraDBInstance {

	/**
	 * Our Niagara database. The different k/v state will share the same niagara instance.
	 *
	 * <p>NOTE: Niagara is implemented a singleton.
	 */
	private NiagaraJNI db;

	public NiagaraDBInstance(NiagaraConfiguration config) {
		int cores = config.getCoreNum();
		// Niagara PS mode is useless in Blink, always not starting RPC.
		boolean startRpc = false;
		String niagaraDBConf = config.getNiagaraDBConf();
		String niagaraLogConf = config.getNiagaraLogConf();
		try {
			this.db = NiagaraJNI.open(cores, startRpc, niagaraDBConf, niagaraLogConf);
		} catch (NiagaraException e) {
			throw new NiagaraDBInitException(cores, startRpc, niagaraDBConf, niagaraLogConf, e.getMessage());
		}
	}

	public NiagaraJNI getNiagaraJNI() {
		return db;
	}

	public void close() {
		if (db != null) {
			NiagaraJNI.close();
			db = null;
		}
	}

	private static class NiagaraDBInitException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		NiagaraDBInitException(int cores, boolean startRpc, String dbConf, String logConf, String message) {
			super("The Niagara DB cannot be initialized with configuration, cores: " + cores +
				", startRpc: " + startRpc +
				", DBconf: " + dbConf +
				", logConf: " + logConf +
				". Niagara reported message: " + message);
		}
	}
}
