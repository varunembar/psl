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

import java.nio.ByteBuffer;

/**
 * A simple response indicating success or failure.
 */
public class Response extends Message {
	boolean success;

	public Response() {
		this(false);
	}

	public Response(boolean success) {
		this.success = success;
	}

	@Override
	protected byte[] serializePayload() {
		return new byte[]{(byte)(success ? 1 : 0)};
	}

	@Override
	protected void deserializePayload(ByteBuffer payload) {
		success = payload.get() == 1;
	}

	@Override
	public String toString() {
		return "Response: " + success;
	}
}
