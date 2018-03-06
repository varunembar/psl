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
public class MaxPiecewisePseudoLikelihood extends VotedPerceptron {
	/**
	 * Prefix of property keys used by this class.
	 */
	public static final String CONFIG_PREFIX = "maxpiecewisepseudolikelihood";
	private static final Logger log = LoggerFactory.getLogger(MaxPiecewisePseudoLikelihood.class);

	/**
	 * Key for positive integer property.
	 * MaxPiecewisePseudoLikelihood will sample this many values to approximate the expectations.
	 */
	public static final String NUM_SAMPLES_KEY = CONFIG_PREFIX + ".numsamples";
	public static final String LOSS_SCALING_FACTOR_KEY = CONFIG_PREFIX + ".lossscalingfactor";
	public static final int NUM_SAMPLES_DEFAULT = 100;
	public static final double LOSS_SCALING_FACTOR_DEFAULT = 0.5;

	private int numSamples;
	private double lossScalingFactor;
	private List<Map<RandomVariableAtom, List<WeightedGroundRule>>> ruleRandomVariableMap;

	// We need an RNG for each thread.
	private Random[] rands;

	public MaxPiecewisePseudoLikelihood(Model model, Database rvDB, Database observedDB, ConfigBundle config) {
		this(model.getRules(), rvDB, observedDB, config);
	}

	public MaxPiecewisePseudoLikelihood(List<Rule> rules, Database rvDB, Database observedDB, ConfigBundle config) {
		super(rules, rvDB, observedDB, false, config);

		numSamples = config.getInt(NUM_SAMPLES_KEY, NUM_SAMPLES_DEFAULT);
		if (numSamples <= 0) {
			throw new IllegalArgumentException("Number of samples must be positive.");
		}

		lossScalingFactor = config.getDouble(LOSS_SCALING_FACTOR_KEY, LOSS_SCALING_FACTOR_DEFAULT);
		if (lossScalingFactor <= 0) {
			throw new IllegalArgumentException("Loss Scaling Factor must be positive.");
		}

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
	 * Compute accuracy for each rule
	 * 
	private void computeRuleAccuracy() {
		ruleAccuracy = new double[mutableRules.size()];

		for (int i = 0; i < mutableRules.size(); i++) {
			Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = ruleRandomVariableMap.get(i);
			int counter = 0;
			int numRules = 0;

			for (RandomVariableAtom atom : groundRuleMap.keySet()) {
				for (WeightedGroundRule groundRule : groundRuleMap.get(atom)) {
					if (groundRule.getIncompatibility() > 0 ) {
						counter++;
					}
					numRules++;
				}
			}
			ruleAccuracy[i] = 0.001;
			log.debug("i" + i + " " + counter + " " + numRules);
		}
	}*/

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

						double energy = 0;
						for (int i = 0; i < groundRules.size(); i++) {
							double incomp = groundRules.get(i).getIncompatibility(atom, sample);
							if(incomp == 0) {
								energy += (-1 * lossScalingFactor);
							}
							else {
								energy += incomp; 
							}
						}

						numerator += Math.exp(-1.0 * weight * energy) * energy;
						denominator += Math.exp(-1.0 * weight * energy);
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
						for (int i = 0; i < groundRules.size(); i++) {
							double incomp = groundRules.get(i).getIncompatibility(atom, sample);
							if(incomp == 0) {
								energy += (-1 * lossScalingFactor);
							}
							else {
								energy += incomp; 
							}
						}
						expInc += Math.exp(weight * energy);
					}

					double obsInc = 0;
					for (int i = 0; i < groundRules.size(); i++) {
						double incomp = groundRules.get(i).getIncompatibility();
						if(incomp == 0) {
							obsInc += (weight * lossScalingFactor);
						}
						else {
							obsInc += (-1 * weight * incomp);
						}
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

			double weight = ((WeightedRule) rule).getWeight();
			double obsInc = 0;

			for (RandomVariableAtom atom : groundRuleMap.keySet()) {
				for (WeightedGroundRule groundRule : groundRuleMap.get(atom)) {
					double incomp = groundRule.getIncompatibility();
					if(incomp == 0) {
						obsInc += (-1 * lossScalingFactor);
					}
					else {
						obsInc += incomp; 
					}
				}
			}

			observedIncompatibility[ruleIndex] = obsInc;
		}
	}
}
