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

import org.linqs.psl.application.inference.distributed.message.ConsensusPartials;
import org.linqs.psl.application.inference.distributed.message.PrimalResidualPartials;
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

	private final int numThreads;

	private ADMMTermStore termStore;
	private volatile double[] consensusValues;

	private List<MinimizeThread> minThreads;
	private CyclicBarrier minBarrier;

	private List<ConsensusPartialsThread> consensusPartialThreads;
	private CyclicBarrier consensusPartialBarrier;

	private List<PrimalResidualsThread> primalResidualsThreads;
	private CyclicBarrier primalResidualsBarrier;

   // Keep a single consensus partials message that will be reused.
   private ConsensusPartials consensusPartialsMessage;

	public ADMMReasonerWorker(ConfigBundle config, ADMMTermStore termStore) {
		stepSize = config.getDouble(STEP_SIZE_KEY, STEP_SIZE_DEFAULT);

		this.termStore = termStore;
		consensusValues = null;
      consensusPartialsMessage = new ConsensusPartials(termStore.getNumGlobalVariables());

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

		// Start up all the consensus partial threads.
		consensusPartialThreads = new ArrayList<ConsensusPartialsThread>();
		consensusPartialBarrier = new CyclicBarrier(numThreads + 1);

		blockSize = (int)(Math.ceil((double)termStore.getNumGlobalVariables() / (double)numThreads));
		for (int i = 0; i < numThreads; i++) {
			ConsensusPartialsThread thread = new ConsensusPartialsThread(blockSize * i, Math.min(blockSize * (i + 1), termStore.getNumGlobalVariables()), consensusPartialBarrier);
			thread.start();
			consensusPartialThreads.add(thread);
		}

		// Start up all the primal residual threads.
		primalResidualsThreads = new ArrayList<PrimalResidualsThread>();
		primalResidualsBarrier = new CyclicBarrier(numThreads + 1);

		blockSize = (int)(Math.ceil((double)termStore.getNumGlobalVariables() / (double)numThreads));
		for (int i = 0; i < numThreads; i++) {
			PrimalResidualsThread thread = new PrimalResidualsThread(blockSize * i, Math.min(blockSize * (i + 1), termStore.getNumGlobalVariables()), primalResidualsBarrier);
			thread.start();
			primalResidualsThreads.add(thread);
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

		if (consensusPartialThreads != null) {
			for (ADMMWorkerThread consensusPartialsThread : consensusPartialThreads) {
				consensusPartialsThread.doWork = false;
				consensusPartialsThread.done = true;
				consensusPartialsThread.interrupt();
				try {
					consensusPartialsThread.join();
				} catch (InterruptedException ex) {
					// Just give up.
				}
			}
			consensusPartialThreads.clear();
			consensusPartialThreads = null;
		}

		if (primalResidualsThreads != null) {
			for (ADMMWorkerThread primalResidualssThread : primalResidualsThreads) {
				primalResidualssThread.doWork = false;
				primalResidualssThread.done = true;
				primalResidualssThread.interrupt();
				try {
					primalResidualssThread.join();
				} catch (InterruptedException ex) {
					// Just give up.
				}
			}
			primalResidualsThreads.clear();
			primalResidualsThreads = null;
		}
	}

	public ConsensusPartials iteration(ADMMTermStore termStore, double[] consensusValues) {
		// TEST
		System.err.println("TEST4: " + consensusValues[0]);

		this.consensusValues = consensusValues;

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

		// Compute consensus partials.
		consensusPartialBarrier.reset();
		for (ADMMWorkerThread thread : consensusPartialThreads) {
			thread.doWork = true;
			thread.interrupt();
		}

		try {
			consensusPartialBarrier.await();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Interrupted while waiting at consensus partials barrier", ex);
		} catch (BrokenBarrierException ex) {
			throw new RuntimeException("Broken barrier during consensus partials update", ex);
		}

		// Grab the computed values from the consensus partials threads.
      consensusPartialsMessage.zero();
		for (ConsensusPartialsThread thread : consensusPartialThreads) {
         for (int i = 0; i < thread.consensusPartials.length; i++) {
            consensusPartialsMessage.values[i + thread.startIndex] += thread.consensusPartials[i];
         }
         
			consensusPartialsMessage.axNormInc += thread.axNormInc;
			consensusPartialsMessage.ayNormInc += thread.ayNormInc;
		}

		return consensusPartialsMessage;
	}

	public PrimalResidualPartials calculatePrimalResiduals(ADMMTermStore termStore, double[] consensusValues) {
		this.consensusValues = consensusValues;

      double primalResInc = 0.0;
      double lagrangePenalty = 0.0;
      double augmentedLagrangePenalty = 0.0;

		// Compute primal redisuals.
		primalResidualsBarrier.reset();
		for (ADMMWorkerThread thread : primalResidualsThreads) {
			thread.doWork = true;
			thread.interrupt();
		}

		try {
			primalResidualsBarrier.await();
		} catch (InterruptedException ex) {
			throw new RuntimeException("Interrupted while waiting at primal residuals barrier", ex);
		} catch (BrokenBarrierException ex) {
			throw new RuntimeException("Broken barrier during primal residuals", ex);
		}

		// Grab the computed values from the threads.
		for (PrimalResidualsThread thread : primalResidualsThreads) {
         primalResInc += thread.primalResInc;
         lagrangePenalty += thread.lagrangePenalty;
         augmentedLagrangePenalty += thread.augmentedLagrangePenalty;
		}

		return new PrimalResidualPartials(primalResInc, lagrangePenalty, augmentedLagrangePenalty);
   }

	/**
	 * There are a couple tasks that can be parallelized, and they share similar workflows.
	 */
	private abstract class ADMMWorkerThread extends Thread {
		public final int startIndex;
		public final int endIndex;
		// Wait at the barrier after any work.
		private final CyclicBarrier barrier;

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

   /**
    * Compute totals that will contribute to the new consensus.
    */
	private class ConsensusPartialsThread extends ADMMWorkerThread {
		public volatile double axNormInc;
		public volatile double ayNormInc;
      // The index into this + startIndex = global variable index.
      public volatile double[] consensusPartials;

		public ConsensusPartialsThread(int startIndex, int endIndex, CyclicBarrier barrier) {
			super(startIndex, endIndex, barrier);

			axNormInc = 0.0;
			ayNormInc = 0.0;
         consensusPartials = new double[endIndex - startIndex];
		}

		@Override
		protected void prework() {
			axNormInc = 0.0;
			ayNormInc = 0.0;

         for (int i = 0; i < consensusPartials.length; i++) {
            consensusPartials[i] = 0;
         }
		}

		@Override
		protected void work(int variableIndex) {
			for (int localVarIndex = 0; localVarIndex < termStore.getLocalVariables(variableIndex).size(); localVarIndex++) {
				LocalVariable localVariable = termStore.getLocalVariables(variableIndex).get(localVarIndex);

				consensusPartials[variableIndex - startIndex] += localVariable.getValue() + localVariable.getLagrange() / stepSize;
				axNormInc += localVariable.getValue() * localVariable.getValue();
				ayNormInc += localVariable.getLagrange() * localVariable.getLagrange();
			}
		}
	}

   /**
    * Compute primal residuals.
    */
	private class PrimalResidualsThread extends ADMMWorkerThread {
		public volatile double primalResInc;
		public volatile double lagrangePenalty;
		public volatile double augmentedLagrangePenalty;

		public PrimalResidualsThread(int startIndex, int endIndex, CyclicBarrier barrier) {
			super(startIndex, endIndex, barrier);

         primalResInc = 0.0;
         lagrangePenalty = 0.0;
         augmentedLagrangePenalty = 0.0;
		}

		@Override
		protected void prework() {
         primalResInc = 0.0;
         lagrangePenalty = 0.0;
         augmentedLagrangePenalty = 0.0;
		}

		@Override
		protected void work(int variableIndex) {
			for (int localVarIndex = 0; localVarIndex < termStore.getLocalVariables(variableIndex).size(); localVarIndex++) {
				LocalVariable localVariable = termStore.getLocalVariables(variableIndex).get(localVarIndex);

				double diff = localVariable.getValue() - consensusValues[variableIndex];
				primalResInc += diff * diff;

				// Compute Lagrangian penalties
				lagrangePenalty += localVariable.getLagrange() * (localVariable.getValue() - consensusValues[variableIndex]);
				augmentedLagrangePenalty += 0.5 * stepSize * Math.pow(localVariable.getValue() - consensusValues[variableIndex], 2);
			}
		}
	}
}
