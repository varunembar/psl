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
package org.linqs.psl.application.inference.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Handle a collection of connections to workers.
 */
// TODO(eriq): All messages will be prefixed with the size of the payload (not including the size).
public class WorkerPool {
	// TODO(eriq): config?
	private static final int DEFAULT_PORT = 1234;
	private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;

	private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

	private List<SocketChannel> workers;
	private Selector readSelector;
	private ResponseIterator activeIterator;

	public WorkerPool(List<String> addresses) {
		workers = new ArrayList<SocketChannel>();
		activeIterator = null;

		try {
			readSelector = Selector.open();
		} catch (IOException ex) {
			throw new RuntimeException("Unable to open selector for worker reads.", ex);
		}

		for (String address : addresses) {
			String[] parts = address.split(":");

			String host = parts[0];
			int port = DEFAULT_PORT;
			if (parts.length == 2) {
				port = Integer.parseInt(parts[1]);
			}

			try {
				SocketChannel socket = SocketChannel.open();
				socket.connect(new InetSocketAddress(host, port));

				// Do not block.
				socket.configureBlocking(false);
				socket.register(readSelector, SelectionKey.OP_READ, new Integer(workers.size()));
				workers.add(socket);
			} catch (IOException ex) {
				throw new RuntimeException("Unable to connect to worker with address: " + address, ex);
			}
		}
	}

	/**
	 * Submit a message to all workers and wait for all workers to respond.
	 */
	public List<Message> blockingSubmit(List<Message> messages) {	
		List<Message> responses = new ArrayList<Message>();
		for (Message response : submit(messages)) {
			responses.add(response);
		}

		return responses;
	}

	/**
	 * Submit the same message to all workers and wait for a response.
	 */
	public List<Message> blockingSubmit(Message message) {
		List<Message> messages = new ArrayList<Message>();
		for (int i = 0; i < workers.size(); i++) {
			messages.add(message);
		}

		return blockingSubmit(messages);
	}

	/**
	 * Do not wait for all workers to respond, instead make the iterator (next()) block until at least one response is ready.
	 */
	public Iterable<Message> submit(List<Message> messages) {
		assert(messages.size() == workers.size());

		if (activeIterator != null) {
			throw new IllegalStateException("Cannot submit messages when there are other messages in progress.");
		}

		ByteBuffer buffer = null;

		// Submit all messages.
		for (int i = 0; i < workers.size(); i++) {
			buffer = NetUtils.sendMessage(messages.get(i), workers.get(i), buffer);
		}

		// The iterator will wait for all responses.
		activeIterator = new ResponseIterator();

		return activeIterator;
	}

	public Iterable<Message> submit(Message message) {
		List<Message> messages = new ArrayList<Message>();
		for (int i = 0; i < workers.size(); i++) {
			messages.add(message);
		}

		return submit(messages);
	}

	public void close() {
		activeIterator = null;

		for (SocketChannel worker : workers) {
			try {
				worker.close();
			} catch (Exception ex) {
				log.warn("Error when closing worker connection", ex);
			}
		}
		workers.clear();
		workers = null;

		try {
			readSelector.close();
		} catch (Exception ex) {
			log.warn("Error when closing worker read sekector", ex);
		}
		readSelector = null;
	}

	private class ResponseIterator implements Iterable<Message>, Iterator<Message> {
		private int numResponses;
		private Queue<Message> responseQueue;
		private boolean[] recievedResponses;

		public ResponseIterator() {
			numResponses = 0;
			responseQueue = new LinkedList<Message>();

			recievedResponses = new boolean[workers.size()];
			for (int i = 0; i < recievedResponses.length; i++) {
				recievedResponses[i] = false;
			}
		}

		public Iterator<Message> iterator() {
			return this;
		}

		public boolean hasNext() {
			return !responseQueue.isEmpty() || numResponses < workers.size();
		}

		public Message next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			if (!responseQueue.isEmpty()) {
				return responseQueue.remove();
			}

			ByteBuffer payloadBuffer = null;

			try {
				boolean done = false;
				while (readSelector.select() > 0) {
					for (SelectionKey selectedKey : readSelector.selectedKeys()) {
						// We only are interested in reading, so it better be ready.
						if (!selectedKey.isReadable()) {
							continue;
						}

						int workerIndex = ((Integer)selectedKey.attachment()).intValue();
						payloadBuffer = NetUtils.readMessage(workers.get(workerIndex), payloadBuffer);

						// Make sure we have not heard from this worker before.
						if (recievedResponses[workerIndex]) {
							log.warn("Recieved multiple responses from a worker.");
							continue;
						}

						recievedResponses[workerIndex] = true;
						responseQueue.add(Message.deserialize(payloadBuffer));
						numResponses++;

						done = true;
					}

					if (done) {
						break;
					}
				}
			} catch (IOException ex) {
				throw new RuntimeException("Error selecting from workers", ex);
			}

			Message response = responseQueue.remove();

			// If we have recieved all responses, prepare for the next request.
			if (responseQueue.isEmpty() && numResponses == workers.size()) {
				activeIterator = null;
			}

			return response;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
