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

import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.database.Database;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.rule.WeightedGroundRule;
import org.linqs.psl.model.rule.WeightedRule;
import org.linqs.psl.application.learning.weight.VotedPerceptron;
import org.linqs.psl.util.Parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Learns weights by optimizing the piecewise-pseudo-log-likelihood of the data using
 * the voted perceptron algorithm.
 */
public class CostPenalizedPPLL extends VotedPerceptron {
	/**
	 * Prefix of property keys used by this class.
	 */
	public static final String CONFIG_PREFIX = "costPenalizedPPLL";
	private static final Logger log = LoggerFactory.getLogger(CostPenalizedPPLL.class);

	/**
	 * Key for positive integer property.
	 * MaxPiecewisePseudoLikelihood will sample this many values to approximate the expectations.
	 */
	public static final String NUM_SAMPLES_KEY = CONFIG_PREFIX + ".numsamples";
	public static final int NUM_SAMPLES_DEFAULT = 100;
	public static final String FALSE_POS_COST_KEY = CONFIG_PREFIX + ".fpcost";
	public static final double FALSE_POS_COST_DEFAULT = -10.0;
	public static final String FALSE_NEG_COST_KEY = CONFIG_PREFIX + ".fncost";
	public static final double FALSE_NEG_COST_DEFAULT = 0.5;

	private int numSamples;
	private double falsePosCost;
	private double falseNegCost;
	private List<Map<RandomVariableAtom, List<WeightedGroundRule>>> ruleRandomVariableMap;

	// We need an RNG for each thread.
	private Random[] rands;

	public CostPenalizedPPLL(Model model, Database rvDB, Database observedDB, ConfigBundle config) {
		this(model.getRules(), rvDB, observedDB, config);
	}

	public CostPenalizedPPLL(List<Rule> rules, Database rvDB, Database observedDB, ConfigBundle config) {
		super(rules, rvDB, observedDB, false, config);

		numSamples = config.getInt(NUM_SAMPLES_KEY, NUM_SAMPLES_DEFAULT);
		if (numSamples <= 0) {
			throw new IllegalArgumentException("Number of samples must be positive.");
		}

		falsePosCost = config.getDouble(FALSE_POS_COST_KEY, FALSE_POS_COST_DEFAULT);
		falseNegCost = config.getDouble(FALSE_NEG_COST_KEY, FALSE_NEG_COST_DEFAULT);

		rands = new Random[Parallel.NUM_THREADS];
		for (int i = 0; i < Parallel.NUM_THREADS; i++) {
			rands[i] = new Random(rand.nextLong());
		}

		ruleRandomVariableMap = null;

		averageSteps = false;
	}

	@Override
	protected void postInitGroundModel() {
		populateRandomVariableMap();
	}


	/**
	 * Create a dictonary for each unground rule.
	 * The dictonary maps random variable atoms to the ground rules it participates in.
	 * */
	private void populateRandomVariableMap() {
		ruleRandomVariableMap = new ArrayList<Map<RandomVariableAtom, List<WeightedGroundRule>>>();

		for (Rule rule : mutableRules) {
			Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = new HashMap<RandomVariableAtom, List<WeightedGroundRule>>();
			for (GroundRule groundRule : groundRuleStore.getGroundRules(rule)) {
				for (GroundAtom atom : groundRule.getAtoms()) {
					if (!(atom instanceof RandomVariableAtom)) {
						continue;
					}

					RandomVariableAtom rva = (RandomVariableAtom)atom;
					if (!groundRuleMap.containsKey(rva)) {
						groundRuleMap.put(rva, new ArrayList<WeightedGroundRule>());
					}

					groundRuleMap.get(atom).add((WeightedGroundRule)groundRule);
				}
			}

			ruleRandomVariableMap.add(groundRuleMap);
		}
	}

	/**
	 * Compute the expected incompatibility using the piecewisepseudolikelihood.
	 * Use Monte Carlo integration to approximate epectations.
	 */
	@Override
	protected void computeExpectedIncompatibility() {
		setLabeledRandomVariables();

		Parallel.count(mutableRules.size(), new Parallel.Worker<Integer>() {
			@Override
			public void work(int ruleIndex, Integer ignore) {
				WeightedRule rule = mutableRules.get(ruleIndex);
				Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = ruleRandomVariableMap.get(ruleIndex);

				double accumulateIncompatibility = 0.0;
				double weight = rule.getWeight();

				for (RandomVariableAtom atom : groundRuleMap.keySet()) {
					List<WeightedGroundRule> groundRules = groundRuleMap.get(atom);

					double numerator = 0.0;
					double denominator = 1e-6;

					for (int sampleIndex = 0; sampleIndex < numSamples; sampleIndex++) {
						double sample = rands[id].nextDouble();

						double energy = 0.0;
						double costFunction = 0.0;

						for (int i = 0; i < groundRules.size(); i++) {
							double incomp = groundRules.get(i).getIncompatibility(atom, sample);
							double atomTruthValue = atom.getValue();

							energy += incomp;

							if(atomTruthValue < 0.5 && sample >= 0.5) {
								costFunction += falsePosCost;
							}
							else if(atomTruthValue >= 0.5 && sample < 0.5){
								costFunction += falseNegCost;
							}
						}

						numerator += Math.exp((-1.0 * weight * energy) + costFunction) * energy;
						denominator += Math.exp((-1.0 * weight * energy) + costFunction);
					}

					accumulateIncompatibility += numerator / denominator;
				}

				expectedIncompatibility[ruleIndex] = accumulateIncompatibility;
			}
		});
	}

	@Override
	public double computeLoss() {
		setLabeledRandomVariables();

		final double[] losses = new double[mutableRules.size()];
		Parallel.count(mutableRules.size(), new Parallel.Worker<Integer>() {
			public void work(int ruleIndex, Integer ignore) {
				Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = ruleRandomVariableMap.get(ruleIndex);
				WeightedRule rule = mutableRules.get(ruleIndex);
				double weight = rule.getWeight();

				for (RandomVariableAtom atom : groundRuleMap.keySet()) {
					List<WeightedGroundRule> groundRules = groundRuleMap.get(atom);

					double expInc = 0;
					for (int sampleIndex = 0; sampleIndex < numSamples; sampleIndex++) {
						double sample = rands[id].nextDouble();

						double energy = 0;
						double costFunction = 0.0;

						for (int i = 0; i < groundRules.size(); i++) {
							double incomp = groundRules.get(i).getIncompatibility(atom, sample);
							energy += incomp;
							double atomTruthValue = atom.getValue();

							if(atomTruthValue < 0.5 && sample >= 0.5) {
								costFunction += falsePosCost;
							}
							else if(atomTruthValue >= 0.5 && sample <= 0.5){
								costFunction += falseNegCost;
							}
						}
						expInc += Math.exp((-1 * weight * energy) + costFunction);
					}

					double obsInc = 0;
					for (int i = 0; i < groundRules.size(); i++) {
						obsInc += groundRules.get(i).getIncompatibility();
					}

					expInc = -1.0 * Math.log(expInc / numSamples);
					losses[ruleIndex] += (obsInc + expInc);
				}

				losses[ruleIndex] += (-0.5 * l2Regularization * Math.pow(weight, 2.0));
			}
		});

		double loss = 0;
		for (double ruleLoss : losses) {
			loss += ruleLoss;
		}

		return loss;
	}

	@Override
	protected void computeObservedIncompatibility() {
		setLabeledRandomVariables();

		for (int ruleIndex = 0; ruleIndex < mutableRules.size(); ruleIndex++) {
			WeightedRule rule = mutableRules.get(ruleIndex);
			Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = ruleRandomVariableMap.get(ruleIndex);

			double obsInc = 0.0;

			for (RandomVariableAtom atom : groundRuleMap.keySet()) {
				for (WeightedGroundRule groundRule : groundRuleMap.get(atom)) {
					obsInc += groundRule.getIncompatibility();

				}
			}

			observedIncompatibility[ruleIndex] = obsInc;
		}
	}
}
