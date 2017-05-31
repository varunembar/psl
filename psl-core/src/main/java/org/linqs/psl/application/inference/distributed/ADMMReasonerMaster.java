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
// TODO(eriq): Change package?
package org.linqs.psl.application.inference.distributed;

// TODO(eriq): Imports
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.WeightedGroundRule;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.ThreadPool;
import org.linqs.psl.reasoner.admm.term.ADMMTermStore;
import org.linqs.psl.reasoner.admm.term.LocalVariable;
import org.linqs.psl.reasoner.term.TermGenerator;
import org.linqs.psl.reasoner.term.TermStore;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
//

import org.linqs.psl.model.atom.PersistedAtomManager;
import org.linqs.psl.model.atom.RandomVariableAtom;

import org.linqs.psl.application.inference.distributed.message.ConsensusPartials;
import org.linqs.psl.application.inference.distributed.message.ConsensusUpdate;
import org.linqs.psl.application.inference.distributed.message.InitADMM;
import org.linqs.psl.application.inference.distributed.message.IterationStart;
import org.linqs.psl.application.inference.distributed.message.Message;
import org.linqs.psl.application.inference.distributed.message.PrimalResidualPartials;
import org.linqs.psl.application.inference.distributed.message.VariableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Master for distributed ADMM.
 */
// TODO(eriq): Interface?
public class ADMMReasonerMaster {
	private static final Logger log = LoggerFactory.getLogger(ADMMReasonerMaster.class);

	/**
	 * Prefix of property keys used by this class.
	 *
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "admmreasoner";

	/**
	 * Key for int property for the maximum number of iterations of ADMM to
	 * perform in a round of inference
	 */
	public static final String MAX_ITER_KEY = CONFIG_PREFIX + ".maxiterations";
	/** Default value for MAX_ITER_KEY property */
	public static final int MAX_ITER_DEFAULT = 25000;

	/**
	 * Key for non-negative double property. Controls step size. Higher
	 * values result in larger steps.
	 */
	public static final String STEP_SIZE_KEY = CONFIG_PREFIX + ".stepsize";
	/** Default value for STEP_SIZE_KEY property */
	public static final double STEP_SIZE_DEFAULT = 1;

	/**
	 * Key for positive double property. Absolute error component of stopping
	 * criteria.
	 */
	public static final String EPSILON_ABS_KEY = CONFIG_PREFIX + ".epsilonabs";
	/** Default value for EPSILON_ABS_KEY property */
	public static final double EPSILON_ABS_DEFAULT = 1e-5;

	/**
	 * Key for positive double property. Relative error component of stopping
	 * criteria.
	 */
	public static final String EPSILON_REL_KEY = CONFIG_PREFIX + ".epsilonrel";
	/** Default value for EPSILON_ABS_KEY property */
	public static final double EPSILON_REL_DEFAULT = 1e-3;

	private static final double LOWER_BOUND = 0.0;
	private static final double UPPER_BOUND = 1.0;

	/**
	 * Sometimes called eta or rho,
	 */
	private final double stepSize;

	private double epsilonRel;
	private double epsilonAbs;

	private int maxIter;

	private WorkerPool workers;

	// TODO(eriq): I would like a better identifier than the full string form.
	//	Hash is no good since it is not perfect (there can be collisions).
	// All of the variables that we have seen.
	// {string: index}.
	private Map<String, Integer> allVariables;

	// The mapping of all consensus variables, to the ones present in each worker.
	// The outer array is keyed by the worker's id.
	// The inner array is a mapping of the worker index to master index.
	// [workerId][workerIndex] = masterIndex
	private int[][] workerVariableMapping;

	// The sum of the counts of local variables reported by each worker.
	// Required for initialization.
	private int numLocalVariables;

	// The total number of local variables across workers for each global variable.
	// Kept as a List instead of int[] because we do not actually know how many variables we have up front.
	private List<Integer> localVariableCount;

	// Keep around the consensus update messages so we don't have to reallocate.
	List<Message> updateMessages;

	private PersistedAtomManager atomManager;

	public ADMMReasonerMaster(ConfigBundle config, WorkerPool workers, PersistedAtomManager atomManager) {
		this.workers = workers;
		this.atomManager = atomManager;

		allVariables = new HashMap<String, Integer>();
		workerVariableMapping = new int[workers.size()][];
		updateMessages = new ArrayList<Message>();

		numLocalVariables = 0;
		localVariableCount = new ArrayList<Integer>();

		maxIter = config.getInt(MAX_ITER_KEY, MAX_ITER_DEFAULT);
		stepSize = config.getDouble(STEP_SIZE_KEY, STEP_SIZE_DEFAULT);

		epsilonAbs = config.getDouble(EPSILON_ABS_KEY, EPSILON_ABS_DEFAULT);
		if (epsilonAbs <= 0) {
			throw new IllegalArgumentException("Property " + EPSILON_ABS_KEY + " must be positive.");
		}

		epsilonRel = config.getDouble(EPSILON_REL_KEY, EPSILON_REL_DEFAULT);
		if (epsilonRel <= 0) {
			throw new IllegalArgumentException("Property " + EPSILON_REL_KEY + " must be positive.");
		}
	}

	private double[] collectVariables() {
		// Have workers collect and send all their variables.
		// Don't wait for all responses, start building the mapping immediately.
		for (Response response : workers.submit(new InitADMM())) {
			if (!(response.getMessage() instanceof VariableList)) {
				throw new RuntimeException("Did not recieve variables from worker as expected.");
			}

			int workerId = response.getWorker();
			VariableList workerVariables = (VariableList)response.getMessage();

			numLocalVariables += workerVariables.getNumLocalVariables();

			workerVariableMapping[workerId] = new int[workerVariables.size()];
			for (int i = 0; i < workerVariables.size(); i++) {
				String key = workerVariables.getVariable(i);
				if (!allVariables.containsKey(key)) {
					allVariables.put(key, allVariables.size());
					localVariableCount.add(new Integer(0));
				}

				int globalIndex = allVariables.get(key).intValue();
				workerVariableMapping[workerId][i] = globalIndex;
				localVariableCount.set(
						globalIndex,
						localVariableCount.get(globalIndex).intValue() + workerVariables.getLocalCount(i));
			}
		}

		// Allocate the messages that we will use to pass the consensus updates.
		for (int i = 0; i < workers.size(); i++) {
			updateMessages.add(new ConsensusUpdate(workerVariableMapping[i].length));
		}

		// Initialize the consensus vector.
		// Also sometimes called 'z'.
		double[] consensusValues = new double[allVariables.size()];
		for (RandomVariableAtom atom : atomManager.getPersistedRVAtoms()) {
			String key = atom.toString();

			// We may not use all random variable atoms.
			if (!allVariables.containsKey(key)) {
				continue;
			}

			consensusValues[allVariables.get(key).intValue()] = atom.getValue();
		}

		return consensusValues;
	}

	private Iterable<Response> updateWorkerConsensus(double[] consensusValues, boolean calcPrimalResidals) {
		// All the update messages have already been allocated, but we need to update them.
		for (int workerId = 0; workerId < workers.size(); workerId++) {
			for (int remoteVariableId = 0; remoteVariableId < workerVariableMapping[workerId].length; remoteVariableId++) {
				((ConsensusUpdate)updateMessages.get(workerId)).setValue(
						remoteVariableId, consensusValues[workerVariableMapping[workerId][remoteVariableId]]);
			}

			((ConsensusUpdate)updateMessages.get(workerId)).calcPrimalResidals = calcPrimalResidals;
		}

		if (!calcPrimalResidals) {
			// Send out the updates and wait for all responses.
			workers.blockingSubmit(updateMessages);
			return null;
		}

		return workers.submit(updateMessages);
	}

	/**
	 * Go through all the workers and count the number of times each variable is used.
	 * We will need then when resolving consensus values.
	 */
	private int[] computeVariableWorkerCount() {
		int[] variableWorkerCount = new int[allVariables.size()];
		for (int i = 0; i < variableWorkerCount.length; i++) {
			variableWorkerCount[i] = 0;
		}

		for (int workerId = 0; workerId < workerVariableMapping.length; workerId++) {
			for (int remoteIndex = 0; remoteIndex < workerVariableMapping[workerId].length; remoteIndex++) {
				variableWorkerCount[workerVariableMapping[workerId][remoteIndex]]++;
			}
		}

		return variableWorkerCount;
	}

	// @Override
	public void optimize() {
		double[] consensusValues = collectVariables();
		double[] newConsensusValues = new double[consensusValues.length];

		// Update (initialize) the consunsus values for each worker.
		// Do not request primal residual calculation.
		updateWorkerConsensus(consensusValues, false);

		// The number of workers that use a specific variable.
		// We need this when updating the consensus value.
		int[] variableWorkerCount = computeVariableWorkerCount();

		// Perform inference.
		double primalRes = Double.POSITIVE_INFINITY;
		double dualRes = Double.POSITIVE_INFINITY;
		double epsilonPrimal = 0.0;
		double epsilonDual = 0.0;
		double epsilonAbsTerm = Math.sqrt(numLocalVariables) * epsilonAbs;
		double axNorm = 0.0;
		double bzNorm = 0.0;
		double ayNorm = 0.0;
		double lagrangePenalty = 0.0;
		double augmentedLagrangePenalty = 0.0;

		int iter = 1;

		while ((primalRes > epsilonPrimal || dualRes > epsilonDual) && iter <= maxIter) {
			primalRes = 0.0;
			dualRes = 0.0;
			axNorm = 0.0;
			bzNorm = 0.0;
			ayNorm = 0.0;
			lagrangePenalty = 0.0;
			augmentedLagrangePenalty = 0.0;

			// Zero out the new consensus values since we will be using them as a sum.
			for (int i = 0; i < newConsensusValues.length; i++) {
				newConsensusValues[i] = 0;
			}

			// Perform an iteration on each worker.
			// Do not wait for all workers to respond.
			// Get the results as they stream in and compute new values.
			for (Response response : workers.submit(new IterationStart())) {
				if (!(response.getMessage() instanceof ConsensusPartials)) {
					throw new RuntimeException("Unexpected response type: " + response.getMessage().getClass().getName());
				}

				ConsensusPartials result = (ConsensusPartials)response.getMessage();

				axNorm += result.axNormInc;
				ayNorm += result.ayNormInc;

				// Sum up consensus partials from each worker's variable set.
				int workerId = response.getWorker();
				for (int remoteIndex = 0; remoteIndex < result.values.length; remoteIndex++) {
					newConsensusValues[workerVariableMapping[workerId][remoteIndex]] += result.values[remoteIndex];
				}
			}

			// Average the consensus values over the number of local copies.
			for (int i = 0; i < newConsensusValues.length; i++) {
				newConsensusValues[i] /= localVariableCount.get(i).intValue();

				if (newConsensusValues[i] < LOWER_BOUND) {
					newConsensusValues[i] = LOWER_BOUND;
				} else if (newConsensusValues[i] > UPPER_BOUND) {
					newConsensusValues[i] = UPPER_BOUND;
				}
			}

			// Calculate the dual residual.
			for (int i = 0; i < newConsensusValues.length; i++) {
				double diff = consensusValues[i] - newConsensusValues[i];
				dualRes += diff * diff * localVariableCount.get(i).intValue();
				bzNorm += newConsensusValues[i] * newConsensusValues[i] * localVariableCount.get(i).intValue();
			}
			dualRes = stepSize * Math.sqrt(dualRes);

			// Solidify the new consensus values.
			double[] temp = consensusValues;
			consensusValues = newConsensusValues;
			newConsensusValues = temp;

			// Pass the workers the new consensus values and request the primal residual partials.
			for (Response response : updateWorkerConsensus(consensusValues, true)) {
				if (!(response.getMessage() instanceof PrimalResidualPartials)) {
					throw new RuntimeException("Unexpected response type: " + response.getMessage().getClass().getName());
				}

				PrimalResidualPartials result = (PrimalResidualPartials)response.getMessage();

				primalRes += result.primalResInc;
				lagrangePenalty += result.lagrangePenalty;
				augmentedLagrangePenalty += result.augmentedLagrangePenalty;
			}

			primalRes = Math.sqrt(primalRes);

			epsilonPrimal = epsilonAbsTerm + epsilonRel * Math.max(Math.sqrt(axNorm), Math.sqrt(bzNorm));
			epsilonDual = epsilonAbsTerm + epsilonRel * Math.sqrt(ayNorm);

			if (iter % 50 == 0) {
				log.trace("Residuals at iter {} -- Primal: {} -- Dual: {}", iter, primalRes, dualRes);
				log.trace("--------- Epsilon primal: {} -- Epsilon dual: {}", epsilonPrimal, epsilonDual);
			}

			iter++;
		}

		log.info("Optimization completed in {} iterations. " +
				"Primal res.: {}, Dual res.: {}", new Object[] {iter, primalRes, dualRes});

		// Update variables
		log.info("Writing results to database.");
		updateAtoms(consensusValues);
	}

	private void updateAtoms(double[] consensusValues) {
		for (RandomVariableAtom atom : atomManager.getPersistedRVAtoms()) {
			String key = atom.toString();

			// We may not use all random variable atoms.
			if (!allVariables.containsKey(key)) {
				continue;
			}

			atom.setValue(consensusValues[allVariables.get(key).intValue()]);
			atom.commitToDB();
		}
	}

	// @Override
	public void close() {
	}
}
