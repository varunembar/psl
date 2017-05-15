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
 */
public class ConsensusUpdate extends Message {
	private double[] consensusValues;

	public ConsensusUpdate() {
	}

	public ConsensusUpdate(int size) {
		consensusValues = new double[size];
	}

   public double getValue(int i) {
      return consensusValues[i];
   }

   public void setValue(int index, double value) {
      consensusValues[index] = value;
   }

	@Override
	protected byte[] serializePayload() {
      ByteBuffer buffer = ByteBuffer.allocate(NetUtils.INT_SIZE  + NetUtils.DOUBLE_SIZE * consensusValues.length);
      buffer.clear();
      buffer.putInt(consensusValues.length);

      for (double value : consensusValues) {
         buffer.putDouble(value);
      }
      buffer.flip();

		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
      int size = payload.getInt();
      consensusValues = new double[size];

      for (int i = 0; i < size; i++) {
         consensusValues[i] = payload.getDouble();
      }
	}

	@Override
	public String toString() {
		return "ConsensusUpdate: " + consensusValues.length;
	}
}
