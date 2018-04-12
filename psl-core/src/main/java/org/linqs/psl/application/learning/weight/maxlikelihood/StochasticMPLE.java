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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.database.Database;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.WeightedGroundRule;
import org.linqs.psl.model.rule.WeightedRule;
import org.linqs.psl.application.groundrulestore.GroundRuleStore;
import org.linqs.psl.application.learning.weight.VotedPerceptron;

/**
 * Learns weights by optimizing the pseudo-log-likelihood of the data using
 * the voted perceptron algorithm.
 * 
 * @author Dhanya Sridhar <dsridhar@ucsc.edu>
 */
public class StochasticMPLE extends VotedPerceptron {

	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "stochasticmple";
	
	/**
	 * Key for positive integer property.
	 * MaxPseudoLikelihood will sample this many values to approximate
	 * the integrals in the marginal computation.
	 */
	public static final String NUM_SAMPLES_KEY = CONFIG_PREFIX + ".numsamples";
	/** Default value for NUM_SAMPLES_KEY */
	public static final int NUM_SAMPLES_DEFAULT = 20;

	public static final String MAX_GROUNDINGS_KEY = CONFIG_PREFIX + ".maxgroundings";
	public static final int MAX_GROUNDINGS_DEFAULT = 500;

	
	/**
	 * Key for positive double property.
	 * Used as minimum width for bounds of integration.
	 */
	public static final String MIN_WIDTH_KEY = CONFIG_PREFIX + ".minwidth";
	/** Default value for MIN_WIDTH_KEY */
	public static final double MIN_WIDTH_DEFAULT = 1e-2;
	

	private final int maxGroundings;
	private final int numSamples;
	private final double minWidth;
	

	private List<Map<RandomVariableAtom, List<WeightedGroundRule>>> ruleRandomVariableMap;
	private Set<RandomVariableAtom> rvAtomSet;
	private List<Map<RandomVariableAtom, List<Integer>>> randomIndicesMap;

	private Random random;
	private int randomSeed = 42;

	/**
	 * Constructor
	 * @param model
	 * @param rvDB
	 * @param observedDB
	 * @param config
	 */
	public StochasticMPLE(Model model, Database rvDB, Database observedDB, ConfigBundle config) {
		super(model.getRules(), rvDB, observedDB, false, config);
		
		numSamples = config.getInt(NUM_SAMPLES_KEY, NUM_SAMPLES_DEFAULT);
		if (numSamples <= 0)
			throw new IllegalArgumentException("Number of samples must be positive integer.");
		minWidth = config.getDouble(MIN_WIDTH_KEY, MIN_WIDTH_DEFAULT);
		if (minWidth <= 0)
			throw new IllegalArgumentException("Minimum width must be positive double.");
		
		maxGroundings = config.getInt(MAX_GROUNDINGS_KEY, MAX_GROUNDINGS_DEFAULT);

		ruleRandomVariableMap = null;
		rvAtomSet = null;
		randomIndicesMap = null;

		random = new Random(randomSeed);
	}

	@Override
	protected void postInitGroundModel() {
		populateRandomVariableMap();
		populateRandomVariableList();
		populateRandomIndices();
	}


	/**
	 * Create a dictonary for each unground rule.
	 * The dictonary maps random variable atoms to the ground rules it participates in.
	 * */
	private void populateRandomVariableMap() {
		ruleRandomVariableMap = new ArrayList<Map<RandomVariableAtom, List<WeightedGroundRule>>>();

		for (WeightedRule rule : mutableRules) {
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

	private void populateRandomVariableList(){
		rvAtomSet = new HashSet<RandomVariableAtom>();
		for (GroundRule groundRule : groundRuleStore.getGroundRules()) {
			for (GroundAtom atom : groundRule.getAtoms()) {
				if (atom instanceof RandomVariableAtom) {
					rvAtomSet.add(((RandomVariableAtom)atom));
				}
			}
		}
	}

	private void populateRandomIndices(){
		randomIndicesMap = new ArrayList<Map<RandomVariableAtom, List<Integer>>>();

		for (int ruleIndex = 0 ; ruleIndex < mutableRules.size(); ruleIndex++){
			Map<RandomVariableAtom, List<WeightedGroundRule>> atomRuleMap = ruleRandomVariableMap.get(ruleIndex);
			Map<RandomVariableAtom, List<Integer>> ruleIndexMap = new HashMap<RandomVariableAtom, List<Integer>>();

			for (RandomVariableAtom atom : atomRuleMap.keySet()){
				int numGroundings = atomRuleMap.get(atom).size();
				List<Integer> randomIndices = new ArrayList<Integer>();

				if(numGroundings > maxGroundings){
					for(int iter = 0; iter < maxGroundings; iter++){
						randomIndices.add(random.nextInt(maxGroundings));
					}	
				}else{
					for(int iter = 0; iter < numGroundings; iter++){
						randomIndices.add(iter);
					}
				}
				ruleIndexMap.put(atom, randomIndices);
			}

			randomIndicesMap.add(ruleIndexMap);

		}

	}
	

	/**
	 * Computes the expected incompatibility using the pseudolikelihood.
	 * Uses Monte Carlo integration to approximate definite integrals,
	 * since they do not admit a closed-form antiderivative.
	 */
	@Override
	protected void computeExpectedIncompatibility() {
		
		// double[] expInc = new double[mutableRules.size()];

		for (int i = 0; i < expectedIncompatibility.length; i++) {
			expectedIncompatibility[i] = 0.0;
		}

		Random random = new Random();


		/* Accumulate the expected incompatibility over all atoms */
		for (RandomVariableAtom atom : rvAtomSet) {
			
			/* Sample numSamples random numbers in the range of integration */
			double[] s;
			
			s = new double[numSamples];
			for (int iSample = 0; iSample < s.length; iSample++) {
				s[iSample] = random.nextDouble();
			}
			
			/* Compute the incompatibility of each sample for each rule */
			HashMap<WeightedRule,double[]> incompatibilities = new HashMap<WeightedRule,double[]>();
			
			/* Saves original state */
			double originalAtomValue = atom.getValue();
			
			/* Computes the probability */

			for(int ruleIndex = 0; ruleIndex < mutableRules.size(); ruleIndex++){
				 Map<RandomVariableAtom, List<WeightedGroundRule>> atomRuleMap = ruleRandomVariableMap.get(ruleIndex);
				 Map<RandomVariableAtom, List<Integer>> randomGroundRuleMap = randomIndicesMap.get(ruleIndex);

				 if(atomRuleMap.containsKey(atom)){
				 	for (Integer index : randomGroundRuleMap.get(atom)) {
				 		WeightedGroundRule groundRule = atomRuleMap.get(atom).get(index);
				 		WeightedRule rule = mutableRules.get(ruleIndex);

				 		if (!incompatibilities.containsKey(rule)){
							incompatibilities.put(rule, new double[s.length]);
						}
						double[] inc = incompatibilities.get(rule);
						for (int iSample = 0; iSample < s.length; iSample++) {
							atom.setValue(s[iSample]);
							inc[iSample] += ((WeightedGroundRule) groundRule).getIncompatibility();
						}
					}
				 }
			}

			atom.setValue(originalAtomValue);

			Arrays.sort(s);

			double width;
			/* Compute the exp incomp and accumulate the partition for the current atom. */
			HashMap<WeightedRule,Double> expIncAtom = new HashMap<WeightedRule,Double>();
			double Z = 0.0;
			for (int j = 0; j < s.length; j++) {
				/* Compute the exponent */
				double sum = 0.0;
				for (Map.Entry<WeightedRule,double[]> e2 : incompatibilities.entrySet()) {
					WeightedRule rule = e2.getKey();
					double[] inc = e2.getValue();
					sum -= rule.getWeight() * inc[j];
				}
				double exp = Math.exp(sum);
				/* Add to partition */
				width = 0.0;
				if(j == 0){
					width = s[j]; 
				}
				else if (j == s.length - 1){
					width = 1.0 - s[j];
				}
				else{
					width = s[j+1] - s[j];
				}

				Z += (exp * width);
				// Z+= exp;

				/* Compute the exp incomp for current atom */
				for (Map.Entry<WeightedRule,double[]> e2 : incompatibilities.entrySet()) {
					WeightedRule rule = e2.getKey();
					if (!expIncAtom.containsKey(rule))
						expIncAtom.put(rule, 0.0);
					double val = expIncAtom.get(rule).doubleValue();
					val += exp * incompatibilities.get(rule)[j];
					expIncAtom.put(rule, val);
				}
			}
			/* Finally, we add to the exp incomp for each rule */ 
			for (int i = 0; i < mutableRules.size(); i++) {
				WeightedRule rule = mutableRules.get(i);
				if (expIncAtom.containsKey(rule))
					if (expIncAtom.get(rule) > 0.0) 
						expectedIncompatibility[i] += expIncAtom.get(rule) / (numSamples*Z);
			}
		}
		
		// setLabeledRandomVariables();
		// return expInc;
	}


	@Override
	protected void computeObservedIncompatibility() {
		setLabeledRandomVariables();

		for (int ruleIndex = 0; ruleIndex < mutableRules.size(); ruleIndex++) {
			WeightedRule rule = mutableRules.get(ruleIndex);
			Map<RandomVariableAtom, List<WeightedGroundRule>> groundRuleMap = ruleRandomVariableMap.get(ruleIndex);
			Map<RandomVariableAtom, List<Integer>> randomGroundRuleMap = randomIndicesMap.get(ruleIndex);

			double obsInc = 0.0;

			for (RandomVariableAtom atom : groundRuleMap.keySet()) {
				for (Integer grIndex : randomGroundRuleMap.get(atom)) {
					obsInc += groundRuleMap.get(atom).get(grIndex).getIncompatibility();

				}
			}

			observedIncompatibility[ruleIndex] = obsInc;
		}
	}

}
