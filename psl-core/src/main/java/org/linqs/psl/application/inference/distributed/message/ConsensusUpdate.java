/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2015 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.linqs.psl.application.inference.distributed.message;

import org.linqs.psl.application.inference.distributed.NetUtils;

import java.nio.ByteBuffer;

/**
 * A message updating a worker with consensus values.
 * Values are indexed matching what the worker reported in its
 * VariableList message.
 * If calcPrimalResidals is true, this indicates that the worker should
 * calcualte the primal residuals and respond with a PrimalResidualPartials.
 */
public class ConsensusUpdate extends DoubleList {
	public boolean calcPrimalResidals;

	public ConsensusUpdate() {
		super();
		calcPrimalResidals = false;
	}

	public ConsensusUpdate(int size) {
		super(size);
		calcPrimalResidals = false;
	}

	@Override
	public int additionalPayloadSize() {
		return NetUtils.INT_SIZE;
	}

	@Override
	public void serializeAdditionalPayload(ByteBuffer buffer) {
		buffer.putInt(calcPrimalResidals ? 1 : 0);
	}

	@Override
	public void deserializeAdditionalPayload(ByteBuffer payload) {
		calcPrimalResidals = (payload.getInt() == 1);
	}
}
