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

import java.nio.charset.Charset;
import java.nio.ByteBuffer;

/**
 * Seriazable objects that will get sent over the wire.
 * All Messages must have a default constructor.
 */
public abstract class Message {
	private static final int DEFAULT_NAME_SIZE = 1024;
	private static final String CHARSET_NAME = "UTF-16";

	public byte[] serialize() {
		byte[] payload = serializePayload();
		ByteBuffer encodedString = encodeString(this.getClass().getName());

		ByteBuffer buffer = ByteBuffer.allocate(payload.length + encodedString.capacity());
		buffer.clear();
		buffer.put(encodedString);
		buffer.put(payload);

		buffer.flip();
		return buffer.array();
	}

	/**
	 * Create a Message from some bare data.
	 * This will pull the class off, construct a new Message (using the default
	 * constructor), and call deserializePayload() on the new class.
	 */
	public static Message deserialize(ByteBuffer buffer) {
		// Pull off the class name.
		String className = decodeString(buffer);

		// Create a new task using the default constructor.
		Message task = null;
		try {
			task = (Message)Class.forName(className).newInstance();
		} catch (ReflectiveOperationException ex) {
			throw new RuntimeException("Unable to construct Message (" + className + ").", ex);
		}

		// Deserialize the payload.
		task.deserializePayload(buffer);

		return task;
	}

	/**
	 * Encode a string for transmission by getting its bytes in a standard encoding
	 * and then prepending the length of the string (bytes of the string).
	 * Inverse of decodeString().
	 */
	public static ByteBuffer encodeString(String str) {
		byte[] strBytes = str.getBytes(Charset.forName(CHARSET_NAME));

		ByteBuffer buffer = ByteBuffer.allocate(strBytes.length + NetUtils.INT_SIZE);
		buffer.clear();
		buffer.putInt(strBytes.length);
		buffer.put(strBytes);
		buffer.flip();

		return buffer;
	}

	/**
	 * Inverse of encodeString().
	 */
	public static String decodeString(ByteBuffer buffer) {
		int size = buffer.getInt();
		byte[] strBytes = new byte[size];
		buffer.get(strBytes);

		return new String(strBytes, Charset.forName(CHARSET_NAME));
	}

	protected abstract byte[] serializePayload();

	/**
	 * Subclasses should utilize the relative nature of the ByteBuffer.
	 */
	protected abstract void deserializePayload(ByteBuffer payload);
}
