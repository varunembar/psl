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

import org.linqs.psl.application.inference.distributed.message.Close;
import org.linqs.psl.application.inference.distributed.message.Initialize;
import org.linqs.psl.application.inference.distributed.message.LoadData;
import org.linqs.psl.application.inference.distributed.message.Message;

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
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.Partition;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Master for a distributed MPE Inference.
 */
// TODO(eriq): I don't really like the "application" package too much, take a look at it.
// TODO(eriq): We should probably have a distributed master abstract base class.
public class DistributedMPEInferenceMaster implements ModelApplication {
	private static final Logger log = LoggerFactory.getLogger(DistributedMPEInferenceMaster.class);

	/**
	 * Prefix of property keys used by this class.
	 *
	 * @see ConfigManager
	 */
	// TODO(eriq): Share prefix with worker?
	public static final String CONFIG_PREFIX = "distributedmpeinference";

	/**
	 * The location of the workers.
	 * There is no default value, this is a required key.
	 */
	public static final String WORKERS_KEY = CONFIG_PREFIX + ".workers";

	// TODO(eriq): For now, only one reasoner is allowed.

	// TODO(eriq): protected or private
	protected Model model;
	protected Database db;
	protected ConfigBundle config;
	protected PersistedAtomManager atomManager;
	protected List<String> workerAddresses;

	// TODO(eriq): Kids through config?
	public DistributedMPEInferenceMaster(Model model, Database db, ConfigBundle config) {
		this.model = model;
		this.db = db;
		this.config = config;

		workerAddresses = config.getList(WORKERS_KEY, null);
		if (workerAddresses == null) {
			throw new IllegalArgumentException("Worker addresses must be specified under the key: " + WORKERS_KEY);
		}

		log.debug("Creating persisted atom mannager.");
		atomManager = new PersistedAtomManager(db);
	}

	// TODO(eriq): Data format needs work: [worker][row][col]
	public FullInferenceResult mpeInference(String partition, String predicate, String[][][] partitionData) {
		assert(partitionData == null || partitionData.length == workerAddresses.size());

		log.debug("Initializing Workers");
		WorkerPool workers = initWorkers();
		log.debug("Workers initialized");

		if (partitionData != null) {
			log.debug("Sending Data To Workers");

			List<Message> loadDataMessages = new ArrayList<Message>();
			for (String[][] data : partitionData) {
				loadDataMessages.add(new LoadData(partition, predicate, data));
			}
			workers.blockingSubmit(loadDataMessages);

			log.debug("Data Sent To Workers");
		}

		ADMMReasonerMaster reasoner = new ADMMReasonerMaster(config, workers, atomManager);

		log.info("Beginning inference.");
		reasoner.optimize();
		log.info("Inference complete. Writing results to Database.");

		// TODO(eriq): We need to get these from the workers.
		/*
		double incompatibility = GroundRules.getTotalWeightedIncompatibility(groundRuleStore.getCompatibilityRules());
		double infeasibility = GroundRules.getInfeasibilityNorm(groundRuleStore.getConstraintRules());
		int size = groundRuleStore.size();
		return new MemoryFullInferenceResult(incompatibility, infeasibility, count, size);
		*/

		log.debug("Closing Workers");
		closeWorkers(workers);
		log.debug("Workers closed");

		return null;
	}

	private WorkerPool initWorkers() {
		WorkerPool workers = new WorkerPool(workerAddresses);
		List<Response> responses = workers.blockingSubmit(new Initialize());

		return workers;
	}

	private void closeWorkers(WorkerPool workers) {
		List<Response> responses = workers.blockingSubmit(new Close());

		workers.close();
		workers = null;
	}

	@Override
	public void close() {
		model=null;
		db = null;
		config = null;
	}

}
