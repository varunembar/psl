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

import java.util.ArrayList;
import java.util.List;

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

	private ADMMTermStore termStore;
	private volatile double[] consensusValues;

	private List<MinimizeThread> minThreads;
	private CyclicBarrier minBarrier;

	private List<VariableUpdateThread> variableThreads;
	private CyclicBarrier variableBarrier;

	public ADMMReasonerWorker(ConfigBundle config, ADMMTermStore termStore) {
		stepSize = config.getDouble(STEP_SIZE_KEY, STEP_SIZE_DEFAULT);

		this.termStore = termStore;
		consensusValues = null;

		numThreads = config.getInt(NUM_THREADS_KEY, NUM_THREADS_DEFAULT);
		if (numThreads <= 0) {
			throw new IllegalArgumentException("Property " + NUM_THREADS_KEY + " must be positive.");
		}

		// Start up all the minimization threads.
		minThreads = new ArrayList<MinimizeThread>();
		minBarrier = new CyclicBarrier(numThreads + 1);

		int blockSize = (int)(Math.ceil((double)termStore.size() / (double)numThreads));
		for (int i = 0; i < numThreads; i++) {
			MinimizeThread thread = new MinimizeThread(blockSize * i, Math.min(blockSize * (i + 1), termStore.size()), minBarrier);
			thread.start();
			minThreads.add(thread);
		}

		// Start up all the variable update threads.
		variableThreads = new ArrayList<VariableUpdateThread>();
		variableBarrier = new CyclicBarrier(numThreads + 1);

		blockSize = (int)(Math.ceil((double)termStore.getNumGlobalVariables() / (double)numThreads));
		for (int i = 0; i < numThreads; i++) {
			VariableUpdateThread thread = new VariableUpdateThread(blockSize * i, Math.min(blockSize * (i + 1), termStore.getNumGlobalVariables()), variableBarrier);
			thread.start();
			variableThreads.add(thread);
		}
	}

	public void close() {
		if (minThreads != null) {
			for (ADMMWorkerThread minThread : minThreads) {
				minThread.doWork = false;
				minThread.done = true;
				minThread.interrupt();
				try {
					minThread.join();
				} catch (InterruptedException ex) {
					// Just give up.
				}
			}
			minThreads.clear();
			minThreads = null;
		}

		if (variableThreads != null) {
			for (ADMMWorkerThread variableThread : variableThreads) {
				variableThread.doWork = false;
				variableThread.done = true;
				variableThread.interrupt();
				try {
					variableThread.join();
				} catch (InterruptedException ex) {
					// Just give up.
				}
			}
			variableThreads.clear();
			variableThreads = null;
		}
	}

	public IterationResult iteration(ADMMTermStore termStore, double[] consensusValues) {
		this.consensusValues = consensusValues;

		double primalResInc = 0.0;
		double dualResInc = 0.0;
		double AxNormInc = 0.0;
		double BzNormInc = 0.0;
		double AyNormInc = 0.0;
		double lagrangePenalty = 0.0;
		double augmentedLagrangePenalty = 0.0;

		// Solve each local function.
		minBarrier.reset();
		for (ADMMWorkerThread thread : minThreads) {
			thread.doWork = true;
			thread.interrupt();
		}

		try {
			minBarrier.await();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Interrupted while waiting at min barrier", ex);
		} catch (BrokenBarrierException ex) {
			throw new RuntimeException("Broken barrier during min", ex);
		}

		// Update consensus values.
		variableBarrier.reset();
		for (ADMMWorkerThread thread : variableThreads) {
			thread.doWork = true;
			thread.interrupt();
		}

		try {
			variableBarrier.await();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Interrupted while waiting at variable barrier", ex);
		} catch (BrokenBarrierException ex) {
			throw new RuntimeException("Broken barrier during variable update", ex);
		}

		// Grab the computed values from the variable threads.
		for (VariableUpdateThread thread : variableThreads) {
			primalResInc += thread.primalResInc;
			dualResInc += thread.dualResInc;
			AxNormInc += thread.AxNormInc;
			BzNormInc += thread.BzNormInc;
			AyNormInc += thread.AyNormInc;
			lagrangePenalty += thread.lagrangePenalty;
			augmentedLagrangePenalty += thread.augmentedLagrangePenalty;
		}

		return new IterationResult(primalResInc, dualResInc,
				AxNormInc, BzNormInc, AyNormInc,
				lagrangePenalty, augmentedLagrangePenalty,
				consensusValues);
	}

	/**
	 * There are a couple tasks that can be parallelized, and they share similar workflows.
	 */
	private abstract class ADMMWorkerThread extends Thread {
		private int startIndex;
		private int endIndex;
		// Wait at the barrier after any work.
		private CyclicBarrier barrier;

		public volatile boolean done;
		public volatile boolean doWork;

		public ADMMWorkerThread(int startIndex, int endIndex, CyclicBarrier barrier) {
			done = false;
			doWork = false;

			this.barrier = barrier;
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		@Override
		public void run() {
			while (!done) {
				// Wait until there is work to be done.
				try {
					// Time doesn't matter, we will be interrupted when there is work to do.
					this.sleep(100000);
				} catch (InterruptedException ex) {
					// Time to wake up!
				}

				// There are several reasons we could have been interrupted,
				// make sure we really need to do work.
				if (!doWork) {
					continue;
				}

				prework();
				for (int i = startIndex; i < endIndex; i++) {
					work(i);
				}

				doWork = false;

				try {
					barrier.await();
				} catch (InterruptedException ex) {
					throw new RuntimeException("Interrupted while waiting at barrier", ex);
				} catch (BrokenBarrierException ex) {
					throw new RuntimeException("Broken barrier after work", ex);
				}
			}
		}

		/**
		 * A chance for children to setup before a round of work.
		 */
		protected void prework() {};

		protected abstract void work(int index);
	}

	private class MinimizeThread extends ADMMWorkerThread {
		public MinimizeThread(int startIndex, int endIndex, CyclicBarrier barrier) {
			super(startIndex, endIndex, barrier);
		}

		@Override
		protected void work(int index) {
			termStore.get(index).updateLagrange(stepSize, consensusValues);
			termStore.get(index).minimize(stepSize, consensusValues);
		}
	}

	private class VariableUpdateThread extends ADMMWorkerThread {
		public volatile double primalResInc = 0.0;
		public volatile double dualResInc = 0.0;
		public volatile double AxNormInc = 0.0;
		public volatile double BzNormInc = 0.0;
		public volatile double AyNormInc = 0.0;
		public volatile double lagrangePenalty = 0.0;
		public volatile double augmentedLagrangePenalty = 0.0;

		public VariableUpdateThread(int startIndex, int endIndex, CyclicBarrier barrier) {
			super(startIndex, endIndex, barrier);

			primalResInc = 0.0;
			dualResInc = 0.0;
			AxNormInc = 0.0;
			BzNormInc = 0.0;
			AyNormInc = 0.0;
			lagrangePenalty = 0.0;
			augmentedLagrangePenalty = 0.0;
		}

		@Override
		protected void prework() {
			primalResInc = 0.0;
			dualResInc = 0.0;
			AxNormInc = 0.0;
			BzNormInc = 0.0;
			AyNormInc = 0.0;
			lagrangePenalty = 0.0;
			augmentedLagrangePenalty = 0.0;
		}

		@Override
		protected void work(int variableIndex) {
			// First pass computes newConsensusValue and dual residual fom all local copies.
			double total = 0.0;
			for (LocalVariable localVariable : termStore.getLocalVariables(variableIndex)) {
				total += localVariable.getValue() + localVariable.getLagrange() / stepSize;

				AxNormInc += localVariable.getValue() * localVariable.getValue();
				AyNormInc += localVariable.getLagrange() * localVariable.getLagrange();
			}

			double newConsensusValue = total / termStore.getLocalVariables(variableIndex).size();
			if (newConsensusValue < LOWER_BOUND) {
				newConsensusValue = LOWER_BOUND;
			} else if (newConsensusValue > UPPER_BOUND) {
				newConsensusValue = UPPER_BOUND;
			}

			double diff = consensusValues[variableIndex] - newConsensusValue;
			// Residual is diff^2 * number of local variables mapped to consensusValues element.
			dualResInc += diff * diff * termStore.getLocalVariables(variableIndex).size();
			BzNormInc += newConsensusValue * newConsensusValue * termStore.getLocalVariables(variableIndex).size();

			consensusValues[variableIndex] = newConsensusValue;

			// Second pass computes primal residuals.
			for (LocalVariable localVariable : termStore.getLocalVariables(variableIndex)) {
				diff = localVariable.getValue() - newConsensusValue;
				primalResInc += diff * diff;

				// Compute Lagrangian penalties
				lagrangePenalty += localVariable.getLagrange() * (localVariable.getValue() - consensusValues[variableIndex]);
				augmentedLagrangePenalty += 0.5 * stepSize * Math.pow(localVariable.getValue() - consensusValues[variableIndex], 2);
			}
		}
	}
}
