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
import org.linqs.psl.reasoner.function.AtomFunctionVariable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// TODO(eriq): Should we have a limit on giant string size?
/**
 * String data must NOT have tab or newlines.
 * For serialization, we will turn the data into a giant string.
 */
public class LoadData extends Message {
	private static final String COL_DELIM = "\t";
	private static final String ROW_DELIM = "\n";

	public String partition;
	public String predicate;
	public String[][] data;

	public LoadData() {
	}

	public LoadData(String partition, String predicate, String[][] data) {
		this.partition = partition;
		this.predicate = predicate;
		this.data = data;
	}

	@Override
	protected byte[] serializePayload() {
		// Build the giant string.
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			if (i != 0) {
				builder.append(ROW_DELIM);
			}

			for (int j = 0; j < data[i].length; j++) {
				if (j != 0) {
					builder.append(COL_DELIM);
				}
				builder.append(data[i][j]);
			}
		}

		ByteBuffer dataBuffer = Message.encodeString(builder.toString());
		ByteBuffer partitionBuffer = Message.encodeString(partition);
		ByteBuffer predicateBuffer = Message.encodeString(predicate);

		ByteBuffer buffer = ByteBuffer.allocate(dataBuffer.limit() + partitionBuffer.limit() + predicateBuffer.limit());
		buffer.clear();
		buffer.put(partitionBuffer);
		buffer.put(predicateBuffer);
		buffer.put(dataBuffer);
		buffer.flip();

		return buffer.array();
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		partition = Message.decodeString(payload);
		predicate = Message.decodeString(payload);

		String dataString = Message.decodeString(payload);
		String[] rows = dataString.split(ROW_DELIM);

		data = new String[rows.length][];
		for (int i = 0; i < rows.length; i++) {
			data[i] = rows[i].split(COL_DELIM);
		}
	}

	@Override
	public String toString() {
		return "Data: (" + partition + ", " + predicate + ")[" + data.length + "]";
	}
}
