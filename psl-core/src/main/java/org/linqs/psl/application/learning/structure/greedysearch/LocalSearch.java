package org.linqs.psl.application.learning.structure.greedysearch;

import org.linqs.psl.application.learning.structure.StructureSelectionApplication;
import org.linqs.psl.application.groundrulestore.GroundRuleStore;
import org.linqs.psl.application.groundrulestore.MemoryGroundRuleStore;
import org.linqs.psl.config.ConfigBundle;
import org.linqs.psl.config.ConfigManager;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.database.Database;
import org.linqs.psl.application.util.Grounding;
import org.linqs.psl.model.formula.Conjunction;
import org.linqs.psl.model.formula.Disjunction;
import org.linqs.psl.model.formula.Negation;
import org.linqs.psl.model.formula.Formula;
import org.linqs.psl.model.formula.Implication;
import org.linqs.psl.model.atom.QueryAtom;
import org.linqs.psl.model.term.Variable;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.predicate.PredicateFactory;
import org.linqs.psl.application.learning.structure.greedysearch.scoring.Scorer;
import org.linqs.psl.application.learning.structure.greedysearch.scoring.WeightedPseudoLogLikelihood;
import org.linqs.psl.application.learning.weight.maxlikelihood.MaxLikelihoodMPE;
import org.linqs.psl.application.learning.weight.maxlikelihood.ConstraintFreeMPLE;
import org.linqs.psl.application.learning.weight.maxlikelihood.StochasticMPLE;
import org.linqs.psl.application.learning.weight.VotedPerceptron;
import org.linqs.psl.model.rule.WeightedRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.rule.logical.WeightedLogicalRule;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.ReasonerFactory;
import org.linqs.psl.reasoner.admm.ADMMReasonerFactory;
import org.linqs.psl.reasoner.term.TermGenerator;
import org.linqs.psl.reasoner.term.TermStore;
import org.linqs.psl.application.learning.weight.TrainingMap;


import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Observable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class for learning the structure of
 * {@link WeightedRule CompatibilityRules} in a {@link Model}
 * from data.
 *
 */


public class LocalSearch extends StructureSelectionApplication {
	
	private static final Logger log = LoggerFactory.getLogger(LocalSearch.class);
	/**
	 * Prefix of property keys used by this class.
	 *
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "localsearch";
	
	public static final String WEIGHT_LEARNING_KEY = CONFIG_PREFIX + ".weightlearner";
	public static final String WEIGHT_LEARNING_DEFAULT = "mle";

	public static final String SCORING_KEY = CONFIG_PREFIX + ".scorer";
	public static final String SCORING_DEFAULT = "pll";

	public static final String MAX_ITERATIONS_KEY = CONFIG_PREFIX + ".iterations";
	public static final int MAX_ITERATIONS_DEFAULT = 10;

	public static final String INIT_RULE_WEIGHT_KEY = CONFIG_PREFIX + ".initweight";
	public static final double INIT_RULE_WEIGHT_DEFAULT = 5.0;

	public static final String SQUARED_POTENTIALS_KEY = CONFIG_PREFIX + ".squared";
	public static final boolean SQUARED_POTENTIALS_DEFAULT = true;

	protected double initRuleWeight;
	protected boolean useSquaredPotentials;
	protected int maxIterations;

	public LocalSearch(Model model, Database rvDB, Database observedDB, ConfigBundle config, Set<Rule> candidateRules) {
		super(model, rvDB, observedDB, config, candidateRules);

		maxIterations = config.getInteger(MAX_ITERATIONS_KEY, MAX_ITERATIONS_DEFAULT);
		initRuleWeight = config.getDouble(INIT_RULE_WEIGHT_KEY, INIT_RULE_WEIGHT_DEFAULT);
		useSquaredPotentials = config.getBoolean(SQUARED_POTENTIALS_KEY, SQUARED_POTENTIALS_DEFAULT);


	}
	
	@Override
	protected void doLearn() {

		VotedPerceptron mle = null;
		// VotedPerceptron mle = new ConstraintFreeMPLE(model, rvDB, observedDB, config);
		Scorer scorer = new WeightedPseudoLogLikelihood(model, rvDB, observedDB, config);

		double bestScore = Double.NEGATIVE_INFINITY;
		double previousBestScore = 0.0;
		double tolerance = 1e-6;
		int iter = 0;

		while(Math.abs(previousBestScore - bestScore) > tolerance && iter < maxIterations){

			previousBestScore = bestScore;

			boolean addRule = true;
			Rule bestRule = null;

			for(Rule candRule : candidateRuleSet){
				double originalWeight = ((WeightedRule)candRule).getWeight();

				log.warn("Trying to add rule : " + candRule);
				model.addRule(candRule);
				mle = new ConstraintFreeMPLE(model, rvDB, observedDB, config);
				// Grounding.groundRule(candRule, trainingMap, groundRuleStore);

				double score = 0.0;
				try{
					mle.learn();
					log.warn("Learning complete");

					score = scorer.scoreModel();
					log.warn("Scoring complete");
				}
				catch(Exception e){
					e.printStackTrace();
				}

				((WeightedRule)candRule).setWeight(originalWeight);

				if(score > bestScore){
					bestRule = candRule;
					addRule = true;
					bestScore = score;
				}

				model.removeRule(candRule);
				// Grounding.removeRule(candRule, groundRuleStore);
				
			}

			Set<Rule> currentModelRules = new HashSet<Rule>();
			for(Rule rule : model.getRules()){
				currentModelRules.add(rule);
			}

			if(!currentModelRules.isEmpty()){
				for(Rule rule : currentModelRules){

					log.warn("Trying to remove rule : " + rule);

					model.removeRule(rule);
					mle = new ConstraintFreeMPLE(model, rvDB, observedDB, config);
					// Grounding.removeRule(rule, groundRuleStore);

					double score = 0.0;
					try{
						mle.learn();
						log.warn("Learning complete");

						score = scorer.scoreModel();
						log.warn("Scoring complete");
					}
					catch(Exception e){
						e.printStackTrace();
					}

					if(score > bestScore){
						bestRule = rule;
						addRule = false;
						bestScore = score;
					}

					model.addRule(rule);
					// Grounding.groundRule(rule, trainingMap, groundRuleStore);
				}
			}

			if(bestRule != null){
				
				if(addRule){
					candidateRuleSet.remove(bestRule);
					model.addRule(bestRule);
					// Grounding.groundRule(bestRule, trainingMap, groundRuleStore);
				}
				else{
					model.removeRule(bestRule);
					// Grounding.removeRule(bestRule, groundRuleStore);	
				}
			}
			
			log.warn("Iteration " + iter + " picked rule:" + bestRule + " with score " + bestScore);

			iter++;
		}

		mle.close();
		scorer.close();
	}
	
}
