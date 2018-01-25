/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2017 The Regents of the University of California
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
package org.linqs.psl.application.learning.weight.maxlikelihood;

import org.linqs.psl.application.learning.weight.VotedPerceptron;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.database.Database;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.rule.UnweightedRule;
import org.linqs.psl.model.rule.WeightedGroundRule;
import org.linqs.psl.model.rule.WeightedRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Learns weights by optimizing the pseudo-log-likelihood of the data using the voted perceptron algorithm.
 * This variant of MPLE does not support constraints.
 *
 * TODO(eriq): Merge this togetehter with a working MPLE (with constraints).
 */
public class ConstraintFreeMPLE extends VotedPerceptron {
	/**
	 * Prefix of property keys used by this class.
	 */
	public static final String CONFIG_PREFIX = "constraintfreemple";

	/**
	 * Key for positive integer property.
	 * MaxPseudoLikelihood will sample this many values to approximate
	 * the integrals in the marginal computation.
	 */
	public static final String NUM_SAMPLES_KEY = CONFIG_PREFIX + ".numsamples";
	public static final int NUM_SAMPLES_DEFAULT = 20;

	private final int numSamples;
	private Random random;
	private Map<RandomVariableAtom, List<WeightedGroundRule>> atomRuleMap;
	private double[] samples;

	// The incompatibility of each sample for each rule.
	Map<WeightedRule, double[]> incompatibilities;
	// Allocations of double[numSamples].
	// We will reuse allocations between rounds.
	List<double[]> allocatedArrays;
	int numAllocatedArrays;

	public ConstraintFreeMPLE(Model model, Database rvDB, Database observedDB, ConfigBundle config) {
		this(model.getRules(), rvDB, observedDB, config);
	}

	public ConstraintFreeMPLE(List<Rule> rules, Database rvDB, Database observedDB, ConfigBundle config) {
		super(rules, rvDB, observedDB, false, config);

		numSamples = config.getInt(NUM_SAMPLES_KEY, NUM_SAMPLES_DEFAULT);
		if (numSamples <= 0) {
			throw new IllegalArgumentException("Number of samples must be a positive integer.");
		}

		for (Rule rule : rules) {
			if (rule instanceof UnweightedRule) {
				throw new IllegalArgumentException("ConstraintFreeMPLE can be only used with no unweighted rules.");
			}
		}

		// TODO(eriq): Seed from config.
		random = new Random();
		atomRuleMap = new HashMap<RandomVariableAtom, List<WeightedGroundRule>>();
		samples = new double[numSamples];

		incompatibilities = new HashMap<WeightedRule, double[]>();
		allocatedArrays = new ArrayList<double[]>();
		numAllocatedArrays = 0;
	}

	@Override
	public void initGroundModel() {
		super.initGroundModel();

		// TODO(eriq): Reconcile this with using a AtomRegisterGroundRuleStore.
		for (GroundRule groundRule : groundRuleStore.getGroundRules()) {
			if (!(groundRule instanceof WeightedGroundRule)) {
				throw new IllegalArgumentException("ConstraintFreeMPLE does not support unweighted ground rules.");
			}

			for (GroundAtom atom : groundRule.getAtoms()) {
				if (!(atom instanceof RandomVariableAtom)) {
					continue;
				}

				RandomVariableAtom rvAtom = (RandomVariableAtom)atom;
				if (!atomRuleMap.containsKey(rvAtom)) {
					atomRuleMap.put(rvAtom, new LinkedList<WeightedGroundRule>());
				}

				atomRuleMap.get(rvAtom).add((WeightedGroundRule)groundRule);
			}
		}
	}

	/**
	 * Computes the expected incompatibility using the pseudolikelihood.
	 * Uses Monte Carlo integration to approximate definite integrals,
	 * since they do not admit a closed-form antiderivative.
	 */
	@Override
	protected void computeExpectedIncompatibility() {
		// Zero out the expected incompatibility first.
		for (int i = 0; i < expectedIncompatibility.length; i++) {
			expectedIncompatibility[i] = 0.0;
		}

		// Accumulate the expected incompatibility over all atoms.
		for (RandomVariableAtom atom : atomRuleMap.keySet()) {
			// Sample numSamples random numbers in the range of integration.
			for (int i = 0; i < samples.length; i++) {
				samples[i] = random.nextDouble();
			}

			freeSampleArrays();
			incompatibilities.clear();

			// Save the original state.
			double originalAtomValue = atom.getValue();

			// Compute the probability.
			for (WeightedGroundRule groundRule : atomRuleMap.get(atom)) {
				if (!incompatibilities.containsKey(groundRule.getRule())) {
					incompatibilities.put(groundRule.getRule(), getSampleArray());
				}

				double[] inc = incompatibilities.get(groundRule.getRule());
				for (int i = 0; i < samples.length; i++) {
					atom.setValue(samples[i]);
					inc[i] += groundRule.getIncompatibility();
				}
			}

			atom.setValue(originalAtomValue);

			Arrays.sort(samples);

			// Compute the exp incomp and accumulate the partition for the current atom.
			HashMap<WeightedRule,Double> expIncAtom = new HashMap<WeightedRule,Double>();
			double Z = 0.0;

			for (int j = 0; j < samples.length; j++) {
				// Compute the exponent.
				double sum = 0.0;
				for (Map.Entry<WeightedRule, double[]> entry : incompatibilities.entrySet()) {
					sum -= entry.getKey().getWeight() * entry.getValue()[j];
				}

				double exp = Math.exp(sum);

				// Add to partition,
				double width = 0.0;
				if (j == 0) {
					width = samples[j];
				} else if (j == samples.length - 1) {
					width = 1.0 - samples[j];
				} else {
					width = samples[j + 1] - samples[j];
				}

				Z += (exp * width);

				// Compute the exp incomp for current atom.
				for (Map.Entry<WeightedRule, double[]> entry : incompatibilities.entrySet()) {
					WeightedRule rule = entry.getKey();
					if (!expIncAtom.containsKey(rule)) {
						expIncAtom.put(rule, 0.0);
					}

					double val = expIncAtom.get(rule).doubleValue();
					val += exp * entry.getValue()[j];
					expIncAtom.put(rule, val);
				}
			}

			// Finally, we add to the exp incomp for each rule.
			for (int i = 0; i < mutableRules.size(); i++) {
				WeightedRule rule = mutableRules.get(i);
				if (expIncAtom.containsKey(rule)) {
					if (expIncAtom.get(rule) > 0.0) {
						expectedIncompatibility[i] += expIncAtom.get(rule) / (numSamples * Z);
					}
				}
			}
		}
	}

	@Override
	public void close() {
		super.close();

		if (atomRuleMap != null) {
			atomRuleMap.clear();
			atomRuleMap = null;
		}

		if (incompatibilities != null) {
			incompatibilities.clear();
			incompatibilities = null;
		}

		if (allocatedArrays != null) {
			allocatedArrays.clear();
			allocatedArrays = null;
		}

		samples = null;
		numAllocatedArrays = 0;
	}

	/**
	 * Give back all the sample arrays.
	 * This does not actually free the memory (that is close()), just marks them as available again.
	 */
	private void freeSampleArrays() {
		numAllocatedArrays = 0;
	}

	/**
	 * Get a zeroed out double[] of size numSamples.
	 * We move this out to a method so we can reuse allocations.
	 * This will not actually pull any samples.
	 */
	private double[] getSampleArray() {
		double[] array = null;

		if (numAllocatedArrays < allocatedArrays.size()) {
			array = allocatedArrays.get(numAllocatedArrays);
		} else {
			array = new double[numSamples];
			allocatedArrays.add(array);
		}

		for (int i = 0; i < array.length; i++) {
			array[i] = 0;
		}

		numAllocatedArrays++;
		return array;
	}
}
