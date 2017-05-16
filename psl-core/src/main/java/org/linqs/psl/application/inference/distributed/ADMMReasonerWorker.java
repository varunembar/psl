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

import org.linqs.psl.application.inference.distributed.message.IterationResult;
import org.linqs.psl.reasoner.admm.term.ADMMObjectiveTerm;


/**
 * Worker for distributed ADMM.
 */
// TODO(eriq): Interface?
public class ADMMReasonerWorker {
	private static final Logger log = LoggerFactory.getLogger(ADMMReasonerWorker.class);

	/**
	 * Prefix of property keys used by this class.
	 *
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "admmreasoner";

	/**
	 * Key for non-negative double property. Controls step size. Higher
	 * values result in larger steps.
	 */
	public static final String STEP_SIZE_KEY = CONFIG_PREFIX + ".stepsize";
	/** Default value for STEP_SIZE_KEY property */
	public static final double STEP_SIZE_DEFAULT = 1;

	/**
	 * Key for positive integer. Number of threads to run the optimization in.
	 */
	public static final String NUM_THREADS_KEY = CONFIG_PREFIX + ".numthreads";
	/** Default value for NUM_THREADS_KEY property
	 * (by default uses the number of processors in the system) */
	public static final int NUM_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();

	private static final double LOWER_BOUND = 0.0;
	private static final double UPPER_BOUND = 1.0;

	/**
	 * Sometimes called eta or rho,
	 */
	private final double stepSize;

	/**
	 * Multithreading variables
	 */
	private final int numThreads;

	public ADMMReasonerWorker(ConfigBundle config) {
		stepSize = config.getDouble(STEP_SIZE_KEY, STEP_SIZE_DEFAULT);

		// Multithreading
		numThreads = config.getInt(NUM_THREADS_KEY, NUM_THREADS_DEFAULT);
		if (numThreads <= 0) {
			throw new IllegalArgumentException("Property " + NUM_THREADS_KEY + " must be positive.");
		}
	}

	// TODO(eriq): We need to multithread this.
	public IterationResult iteration(ADMMTermStore termStore, double[] consensusValues) {
		double primalResInc = 0.0;
		double dualResInc = 0.0;
		double AxNormInc = 0.0;
		double BzNormInc = 0.0;
		double AyNormInc = 0.0;
		double lagrangePenalty = 0.0;
		double augmentedLagrangePenalty = 0.0;

		// Solve each local function.
		for (ADMMObjectiveTerm term : termStore) {
			term.updateLagrange(stepSize, consensusValues);
			term.minimize(stepSize, consensusValues);
		}

		// TODO(eriq): Non-distributed ADMM can go multiple iterations without checking. Can we?

		for (int i = 0; i < termStore.getNumGlobalVariables(); i++) {
			double total = 0.0;

			// First pass computes newConsensusValue and dual residual fom all local copies.
			for (LocalVariable localVariable : termStore.getLocalVariables(i)) {
				total += localVariable.getValue() + localVariable.getLagrange() / stepSize;

				AxNormInc += localVariable.getValue() * localVariable.getValue();
				AyNormInc += localVariable.getLagrange() * localVariable.getLagrange();
			}

			double newConsensusValue = total / termStore.getLocalVariables(i).size();
			if (newConsensusValue < LOWER_BOUND) {
				newConsensusValue = LOWER_BOUND;
			} else if (newConsensusValue > UPPER_BOUND) {
				newConsensusValue = UPPER_BOUND;
			}

			double diff = consensusValues[i] - newConsensusValue;
			// Residual is diff^2 * number of local variables mapped to consensusValues element.
			dualResInc += diff * diff * termStore.getLocalVariables(i).size();
			BzNormInc += newConsensusValue * newConsensusValue * termStore.getLocalVariables(i).size();

			consensusValues[i] = newConsensusValue;

			// Second pass computes primal residuals.
			for (LocalVariable localVariable : termStore.getLocalVariables(i)) {
				diff = localVariable.getValue() - newConsensusValue;
				primalResInc += diff * diff;

				// Compute Lagrangian penalties
				lagrangePenalty += localVariable.getLagrange() * (localVariable.getValue() - consensusValues[i]);
				augmentedLagrangePenalty += 0.5 * stepSize * Math.pow(localVariable.getValue() - consensusValues[i], 2);
			}
		}

		return new IterationResult(primalResInc, dualResInc,
				AxNormInc, BzNormInc, AyNormInc,
				lagrangePenalty, augmentedLagrangePenalty,
				consensusValues);
	}
}
