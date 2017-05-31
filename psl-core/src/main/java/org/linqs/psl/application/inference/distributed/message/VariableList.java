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
import org.linqs.psl.reasoner.admm.term.ADMMTermStore;
import org.linqs.psl.reasoner.function.AtomFunctionVariable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// TODO(eriq): I do not like using the full string represetation, but the hash is not good enough.
/**
 * A response containing a list of variables represented by the
 * string represetation of their associated ground atom as well as the number of local variables
 * present on the worker.
 */
public class VariableList extends Message {
	private String[] variables;
   private int[] localCounts;
	private int numLocalVariables;

	public VariableList() {
	}

   /**
    * Note that we are storing the string representation of the atom associated with a variable.
    */
	public VariableList(ADMMTermStore termStore) {
      this.numLocalVariables = termStore.getNumLocalVariables();

      localCounts = new int[termStore.getNumGlobalVariables()];
      for (int i = 0; i < localCounts.length; i++) {
         localCounts[i] = termStore.getLocalVariables(i).size();
      }

      AtomFunctionVariable[] rawVariables = termStore.getGlobalVariables();
		variables = new String[rawVariables.length];
      for (int i = 0; i < variables.length; i++) {
         variables[i] = rawVariables[i].getAtom().toString();
      }
	}

	public int size() {
		return variables.length;
	}

	public String getVariable(int i) {
		return variables[i];
	}

	public int getLocalCount(int i) {
		return localCounts[i];
	}

	public int getNumLocalVariables() {
		return numLocalVariables;
	}

	@Override
	protected byte[] serializePayload() {
      List<ByteBuffer> stringBuffers = new ArrayList<ByteBuffer>();
      int totalStringSize = 0;
      for (String variable : variables) {
         ByteBuffer stringBuffer = Message.encodeString(variable);
         stringBuffers.add(stringBuffer);
         totalStringSize += stringBuffer.limit();
      }

		// The number of strings, the number of local vairbales, the local variable counts, and all the strings.
		ByteBuffer buffer = ByteBuffer.allocate(NetUtils.INT_SIZE * (2 + variables.length) + totalStringSize);
		buffer.clear();
		buffer.putInt(numLocalVariables);
		buffer.putInt(variables.length);

      for (int localCount : localCounts) {
         buffer.putInt(localCount);
      }

		for (ByteBuffer stringBuffer : stringBuffers) {
			buffer.put(stringBuffer);
		}
		buffer.flip();

		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		numLocalVariables = payload.getInt();
		int size = payload.getInt();

      localCounts = new int[size];
		for (int i = 0; i < size; i++) {
         localCounts[i] = payload.getInt();
      }

		variables = new String[size];
		for (int i = 0; i < size; i++) {
			variables[i] = Message.decodeString(payload);
		}
	}

	@Override
	public String toString() {
		return "VariableList: (" + variables.length + ", " + numLocalVariables + ")";
	}
}
