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
 * The result of a round of iteration.
 */
// TODO(eriq): Share code with Consensusupdate?
public class IterationResult extends Message {
	public double primalResInc;
	public double dualResInc;
	public double AxNormInc;
	public double BzNormInc;
	public double AyNormInc;
	public double lagrangePenalty;
	public double augmentedLagrangePenalty;
	public double[] consensusValues;

	public IterationResult() {
	}

	public IterationResult(double primalResInc, double dualResInc,
			double AxNormInc, double BzNormInc, double AyNormInc,
			double lagrangePenalty, double augmentedLagrangePenalty,
			double[] consensusValues) {
		this.primalResInc = primalResInc;
		this.dualResInc = dualResInc;
		this.AxNormInc = AxNormInc;
		this.BzNormInc = BzNormInc;
		this.AyNormInc = AyNormInc;
		this.lagrangePenalty = lagrangePenalty;
		this.augmentedLagrangePenalty = augmentedLagrangePenalty;
		this.consensusValues = consensusValues;
	}

	@Override
	protected byte[] serializePayload() {
		// One int for the consensus size, 7 doubles for the other values, and then the consensus values.
		ByteBuffer buffer = ByteBuffer.allocate(NetUtils.INT_SIZE + NetUtils.DOUBLE_SIZE * (7 + consensusValues.length));
		buffer.clear();

		buffer.putDouble(primalResInc);
		buffer.putDouble(dualResInc);
		buffer.putDouble(AxNormInc);
		buffer.putDouble(BzNormInc);
		buffer.putDouble(AyNormInc);
		buffer.putDouble(lagrangePenalty);
		buffer.putDouble(augmentedLagrangePenalty);

		buffer.putInt(consensusValues.length);
		for (double value : consensusValues) {
			buffer.putDouble(value);
		}

		buffer.flip();

		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		primalResInc = payload.getDouble();
		dualResInc = payload.getDouble();
		AxNormInc = payload.getDouble();
		BzNormInc = payload.getDouble();
		AyNormInc = payload.getDouble();
		lagrangePenalty = payload.getDouble();
		augmentedLagrangePenalty = payload.getDouble();

		int size = payload.getInt();
		consensusValues = new double[size];
		for (int i = 0; i < size; i++) {
			consensusValues[i] = payload.getDouble();
		}
	}

	@Override
	public String toString() {
		return "IterationResult: " + consensusValues.length;
	}
}
