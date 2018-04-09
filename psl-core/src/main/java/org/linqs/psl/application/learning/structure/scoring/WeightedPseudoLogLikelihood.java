package org.linqs.psl.application.learning.structure.greedysearch.scoring;

import java.lang.Math;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;

import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.database.Database;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.WeightedGroundRule;
import org.linqs.psl.model.rule.WeightedRule;
import org.linqs.psl.model.rule.logical.WeightedLogicalRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.application.groundrulestore.GroundRuleStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class WeightedPseudoLogLikelihood extends Scorer{

	private static final Logger log = LoggerFactory.getLogger(WeightedPseudoLogLikelihood.class);
	
	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "wpll";
	
	/**
	 * Key for positive double property scaling the L2 regularization
	 * (\lambda / 2) * ||w||^2
	 */
	public static final String L2_REGULARIZATION_KEY = CONFIG_PREFIX + ".l2regularization";
	/** Default value for L2_REGULARIZATION_KEY */
	public static final double L2_REGULARIZATION_DEFAULT = 0.0;
	
	/**
	 * Key for positive double property scaling the L1 regularization
	 * \gamma * |w|
	 */
	public static final String L1_REGULARIZATION_KEY = CONFIG_PREFIX + ".l1regularization";
	/** Default value for L1_REGULARIZATION_KEY */
	public static final double L1_REGULARIZATION_DEFAULT = 0.0;

	public static final String GRIDSIZE_KEY = CONFIG_PREFIX + ".gridsize";
	public static final int GRIDSIZE_DEFAULT = 5;

	public static final String MAX_GROUNDINGS_KEY = CONFIG_PREFIX + ".maxgroundings";
	public static final int MAX_GROUNDINGS_DEFAULT = 100;

	public static final String RANDOM_SEED_KEY = CONFIG_PREFIX + ".randomseed";
	public static final int RANDOM_SEED_DEFAULT = 42;


	/**
	 * Key for Boolean property that indicates whether to scale pseudolikelihood by number of groundings per predicate
	 */
	public static final String SCALE_PLL_KEY = CONFIG_PREFIX + ".scalepll";
	/** Default value for SCALE_GRADIENT_KEY */
	public static final boolean SCALE_PLL_DEFAULT = true;
		
	protected final double l2Regularization;
	protected final double l1Regularization;
	protected final boolean scalePLL;
	protected final int gridSize;
	protected final int maxAtomGroundRules;
	protected final int randomSeed;
	protected final Random random;

	private Map<RandomVariableAtom, List<WeightedGroundRule>> atomRuleMap;


	public WeightedPseudoLogLikelihood(Model model, Database rvDB, Database observedDB, ConfigBundle config) {
		super(model, rvDB, observedDB, config);
		
		l2Regularization = config.getDouble(L2_REGULARIZATION_KEY, L2_REGULARIZATION_DEFAULT);
		if (l2Regularization < 0)
			throw new IllegalArgumentException("L2 regularization parameter must be non-negative.");

		l1Regularization = config.getDouble(L1_REGULARIZATION_KEY, L1_REGULARIZATION_DEFAULT);
		if (l1Regularization < 0)
			throw new IllegalArgumentException("L1 regularization parameter must be non-negative.");

		gridSize = config.getInteger(GRIDSIZE_KEY, GRIDSIZE_DEFAULT);
		if (gridSize < 0)
			throw new IllegalArgumentException("Gridsize must be non-negative.");

		scalePLL = config.getBoolean(SCALE_PLL_KEY, SCALE_PLL_DEFAULT);
		maxAtomGroundRules = config.getInteger(MAX_GROUNDINGS_KEY, MAX_GROUNDINGS_DEFAULT);

		randomSeed = config.getInteger(RANDOM_SEED_KEY, RANDOM_SEED_DEFAULT);

		random = new Random(randomSeed);

		atomRuleMap = new HashMap<RandomVariableAtom, List<WeightedGroundRule>>();
	}

	public WeightedPseudoLogLikelihood(Model model, Database rvDB, Database observedDB, ConfigBundle config, GroundRuleStore groundRuleStore) {
		super(model, rvDB, observedDB, config, groundRuleStore);
		
		l2Regularization = config.getDouble(L2_REGULARIZATION_KEY, L2_REGULARIZATION_DEFAULT);
		if (l2Regularization < 0)
			throw new IllegalArgumentException("L2 regularization parameter must be non-negative.");

		l1Regularization = config.getDouble(L1_REGULARIZATION_KEY, L1_REGULARIZATION_DEFAULT);
		if (l1Regularization < 0)
			throw new IllegalArgumentException("L1 regularization parameter must be non-negative.");

		gridSize = config.getInteger(GRIDSIZE_KEY, GRIDSIZE_DEFAULT);
		if (gridSize < 0)
			throw new IllegalArgumentException("Gridsize must be non-negative.");

		scalePLL = config.getBoolean(SCALE_PLL_KEY, SCALE_PLL_DEFAULT);
		maxAtomGroundRules = config.getInteger(MAX_GROUNDINGS_KEY, MAX_GROUNDINGS_DEFAULT);

		randomSeed = config.getInteger(RANDOM_SEED_KEY, RANDOM_SEED_DEFAULT);

		random = new Random(randomSeed);


		atomRuleMap = new HashMap<RandomVariableAtom, List<WeightedGroundRule>>();
	}


	@Override
	protected void initGroundModel() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		super.initGroundModel();

		atomRuleMap = new HashMap<RandomVariableAtom, List<WeightedGroundRule>>();

		for (GroundRule groundRule : groundRuleStore.getGroundRules()) {
			if (!(groundRule instanceof WeightedGroundRule)) {
				throw new IllegalArgumentException("WPLL only supports weighted ground rules.");
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



	@Override
	protected double doScoring() {

		double incomp = 0.0;
		double marginalProduct = 0;
		double numRV = 0;

		for(RandomVariableAtom a : atomRuleMap.keySet()) {
			double currentIncomp = computeObservedIncomp(a);
			incomp += currentIncomp;
			double marginal = computeMarginal(a);
			marginalProduct += marginal;
		}

		double l2_norm = -1 * l2_norm() * l2Regularization;
		double struct_norm = -1 * num_predicates() * l1Regularization;
		double pll = -1 * (incomp + marginalProduct) + l2_norm + struct_norm;
		
		log.info(model.toString());
		log.info("Score: " + pll);
		System.out.println("Score: " + pll);

		return pll;
	}

	protected double l2_norm() {

	 	double l2norm = 0.0;

	 	Iterable<Rule> rules = model.getRules();
	 	for(Rule r : rules) {
	 		double w = ((WeightedRule)r).getWeight();
	 		l2norm += w*w;
	 	}

	 	return l2norm;
	}

	protected double num_predicates() {

	 	double numPredicates = 0;
	 	Iterable<Rule> rules = model.getRules();
	 	for (Rule r: rules) {
			Set<Atom> predicates = new HashSet<Atom>();
	 		predicates = ((WeightedLogicalRule)r).getFormula().getAtoms(predicates);
			numPredicates += predicates.size();
	 	}

	 	return numPredicates;
	}


	protected double computeObservedIncomp() {
		Iterable<Rule> rules = model.getRules();
		double truthIncompatibility = 0;
		
		/* Computes the observed incompatibilities and numbers of groundings  */
		for (Rule r: rules) {
			for (GroundRule groundRule : groundRuleStore.getGroundRules(r)) {
				truthIncompatibility += ((WeightedGroundRule) groundRule).getWeight() * ((WeightedGroundRule) groundRule).getIncompatibility();
			}
		}
		return truthIncompatibility;
	}



	protected double computeObservedIncomp(RandomVariableAtom atom) {
		double truthIncompatibility = 0;
		
		List<WeightedGroundRule> atomGroundings = atomRuleMap.get(atom);
		// Random random = new Random();

		if(atomGroundings.size() <= maxAtomGroundRules)
			/* Computes the observed incompatibilities for only one atom  */
			for (GroundRule groundRule : atomRuleMap.get(atom)) {
				truthIncompatibility += ((WeightedGroundRule) groundRule).getWeight() * ((WeightedGroundRule) groundRule).getIncompatibility();
			}
		else{
			int count = 0;
			while(count <= maxAtomGroundRules){
				int randIndex = random.nextInt(atomGroundings.size());
				GroundRule groundRule = atomGroundings.get(randIndex);
				truthIncompatibility += ((WeightedGroundRule) groundRule).getWeight() * ((WeightedGroundRule) groundRule).getIncompatibility();
				count++;
			}
			
		}
		
		return truthIncompatibility;
	}


	protected double computeMarginal(RandomVariableAtom a) {
		
		double cumSum = 0.0;
		double step = 1.0 / gridSize; 

		double currValue = a.getValue();
		double incompatibility[] = new double[gridSize];
		double max = Double.NEGATIVE_INFINITY;
		double totalIncompatibility = 0;

		for (int i = 0; i < gridSize; i++) {
		       a.setValue(i*step);
		       incompatibility[i] = -1*computeObservedIncomp(a);
		       if(incompatibility[i] > max) {
			      max = incompatibility[i];
			} 
		}	       

		for (int i = 0; i < gridSize; i++) {
			totalIncompatibility += Math.exp(incompatibility[i] - max);
		}
		totalIncompatibility = max + Math.log(totalIncompatibility + 1e-6) + Math.log(step + 1e-6);
			
		a.setValue(currValue);
		return totalIncompatibility;
	}
	
	protected double computeRegularizer() {
		double l2 = 0;
		double l1 = 0;
		for (int i = 0; i < kernels.size(); i++) {
			l2 += Math.pow(kernels.get(i).getWeight(), 2);
			l1 += Math.abs(kernels.get(i).getWeight());
		}
		return 0.5 * l2Regularization * l2 + l1Regularization * l1;
	}
}

