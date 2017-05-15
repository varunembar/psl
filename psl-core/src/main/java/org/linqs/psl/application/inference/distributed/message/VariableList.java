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
 * A response containing a list of variables represented by the
 * hash of their associated ground atom.
 */
public class VariableList extends Message {
	private int[] variables;

	public VariableList() {
	}

	public VariableList(int size) {
		variables = new int[size];
	}

   public int size() {
      return variables.length;
   }

   public int getVariable(int i) {
      return variables[i];
   }

	@Override
	protected byte[] serializePayload() {
      ByteBuffer buffer = ByteBuffer.allocate(NetUtils.INT_SIZE * (variables.length + 1));
      buffer.clear();
      buffer.putInt(variables.length);

      for (int variable : variables) {
         buffer.putInt(variable);
      }
      buffer.flip();

		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
      int size = payload.getInt();
      variables = new int[size];

      for (int i = 0; i < size; i++) {
         variables[i] = payload.getInt();
      }
	}

	@Override
	public String toString() {
		return "VariableList: " + variables.length;
	}
}
