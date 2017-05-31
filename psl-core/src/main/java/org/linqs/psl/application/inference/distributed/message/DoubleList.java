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
 * The base for a message that have a list of doubles.
 */
public abstract class DoubleList extends Message {
	public double[] values;

	public DoubleList() {
   }

	public DoubleList(double[] values) {
		this.values = values;
	}

	public DoubleList(int size) {
		values = new double[size];
	}

	public double[] getValues() {
		return values;
	}

   /**
    * Set all values to zero.
    */
	public void zero() {
      for (int i = 0; i < values.length; i++) {
         values[i] = 0.0;
      }
	}

	public double getValue(int i) {
		return values[i];
	}

	public void setValue(int index, double value) {
		values[index] = value;
	}

	@Override
	protected byte[] serializePayload() {
		ByteBuffer buffer = ByteBuffer.allocate(NetUtils.INT_SIZE  + NetUtils.DOUBLE_SIZE * values.length + additionalPayloadSize());
		buffer.clear();
		buffer.putInt(values.length);

		for (double value : values) {
			buffer.putDouble(value);
		}

      serializeAdditionalPayload(buffer);

		buffer.flip();
		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		int size = payload.getInt();
		values = new double[size];
		for (int i = 0; i < size; i++) {
			values[i] = payload.getDouble();
		}

      deserializeAdditionalPayload(payload);
	}

	@Override
	public String toString() {
		return "DoubleList (" + this.getClass().getName() + "): " + values.length;
	}

   /**
    * The size of any additional data that needs to be serialized.
    */
   public abstract int additionalPayloadSize();
   public abstract void serializeAdditionalPayload(ByteBuffer buffer);
   public abstract void deserializeAdditionalPayload(ByteBuffer payload);
}
