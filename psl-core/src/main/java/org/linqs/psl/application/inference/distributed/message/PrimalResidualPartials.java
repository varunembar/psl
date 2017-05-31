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
 * Primal residuals to be sent to the master to sum up.
 */
public class PrimalResidualPartials extends Message {
	public double primalResInc;
	public double lagrangePenalty;
	public double augmentedLagrangePenalty;

	public PrimalResidualPartials() {
	}

	public PrimalResidualPartials(double primalResInc,
		double lagrangePenalty, double augmentedLagrangePenalty) {
		this.primalResInc = primalResInc;
		this.lagrangePenalty = lagrangePenalty;
		this.augmentedLagrangePenalty = augmentedLagrangePenalty;
	}

	@Override
	protected byte[] serializePayload() {
		ByteBuffer buffer = ByteBuffer.allocate(NetUtils.DOUBLE_SIZE * 3);
		buffer.clear();

		buffer.putDouble(primalResInc);
		buffer.putDouble(lagrangePenalty);
		buffer.putDouble(augmentedLagrangePenalty);

		buffer.flip();
		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		primalResInc = payload.getDouble();
		lagrangePenalty = payload.getDouble();
		augmentedLagrangePenalty = payload.getDouble();
	}

	@Override
	public String toString() {
		return "PrimalResidualPartials";
	}
}
