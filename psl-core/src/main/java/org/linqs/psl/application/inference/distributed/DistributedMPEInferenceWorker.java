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

// TODO(eriq): Clean imports
import org.linqs.psl.application.groundrulestore.GroundRuleStore;
import org.linqs.psl.application.ModelApplication;
import org.linqs.psl.application.inference.result.FullInferenceResult;
import org.linqs.psl.application.inference.result.memory.MemoryFullInferenceResult;
import org.linqs.psl.application.util.GroundRules;
import org.linqs.psl.application.util.Grounding;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.config.Factory;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DatabasePopulator;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.PersistedAtomManager;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.ReasonerFactory;
import org.linqs.psl.reasoner.admm.ADMMReasonerFactory;
import org.linqs.psl.reasoner.term.TermGenerator;
import org.linqs.psl.reasoner.term.TermStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A distributed worker.
 */
// TODO(eriq): Do we need this implemenation?
public class DistributedMPEInferenceWorker implements ModelApplication {
	private static final Logger log = LoggerFactory.getLogger(DistributedMPEInferenceWorker.class);

	/**
	 * Prefix of property keys used by this class.
	 *
	 * @see ConfigManager
	 */
	// TODO(eriq): Share prefix with master?
	public static final String CONFIG_PREFIX = "distributedmpeinference";

	/**
	 * The port that workers listen on.
	 */
	public static final String PORT_KEY = CONFIG_PREFIX + ".port";
	public static final int PORT = 1234;

	// TODO(eriq): For now, only one reasoner is allowed.

	// TODO(eriq): protected or private
	protected Model model;
	protected Database db;
	protected ConfigBundle config;
	protected PersistedAtomManager atomManager;
	protected ServerSocket server;

	// TODO(eriq): Kids through config?
	// TODO(eriq): Get model and db over wire?
	public DistributedMPEInferenceWorker(Model model, Database db, ConfigBundle config) {
		this.model = model;
		this.db = db;
		this.config = config;

		try {
			server = new ServerSocket(PORT);
		} catch (IOException ex) {
			throw new RuntimeException("Failed to create socket for listening.", ex);
		}

		log.debug("Creating persisted atom mannager.");
		atomManager = new PersistedAtomManager(db);
	}

	/**
	 * Listen for connections from a master.
	 * This call will block until a master connects and releases the worker.
	 */
	public void listen() {
		Socket master = null;
		InputStream inStream = null;

		try {
			master = server.accept();
			inStream = master.getInputStream();
		} catch (IOException ex) {
			throw new RuntimeException("Unable to accept connection from master.", ex);
		}

		ByteBuffer buffer = null;

		// Accept messages from the master until it closes.
		while (true) {
			buffer = NetUtils.readMessage(inStream, buffer);
			Message message = Message.deserialize(buffer);

			// TEST
			System.out.println(message);
		}
	}

	@Override
	public void close() {
		model=null;
		db = null;
		config = null;
	}

}
