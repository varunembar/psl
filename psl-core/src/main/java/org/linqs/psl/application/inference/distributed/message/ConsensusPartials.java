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
 * A message from a worker to a master with informaion on how to update the consensus values.
 */
public class ConsensusPartials extends DoubleList {
   public double axNormInc;
   public double ayNormInc;

	public ConsensusPartials() {
      super();
   }

	public ConsensusPartials(int size) {
      super(size);
	}

	@Override
	public void zero() {
      axNormInc = 0;
      ayNormInc = 0;
      super.zero();
   }

	@Override
   public int additionalPayloadSize() {
      return NetUtils.DOUBLE_SIZE * 2;
   }

	@Override
   public void serializeAdditionalPayload(ByteBuffer buffer) {
      buffer.putDouble(axNormInc);
      buffer.putDouble(ayNormInc);
   }

	@Override
   public void deserializeAdditionalPayload(ByteBuffer payload) {
      axNormInc = payload.getDouble();
      ayNormInc = payload.getDouble();
   }
}
