/*
 * This file is part of the PSL software.
 * Copyright 2011 University of Maryland
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
package edu.umd.cs.psl.reasoner.admm;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;
import edu.umd.cs.psl.model.atom.Atom;
import edu.umd.cs.psl.model.kernel.CompatibilityKernel;
import edu.umd.cs.psl.model.kernel.GroundCompatibilityKernel;
import edu.umd.cs.psl.model.kernel.GroundConstraintKernel;
import edu.umd.cs.psl.model.kernel.GroundKernel;
import edu.umd.cs.psl.model.kernel.Kernel;
import edu.umd.cs.psl.reasoner.Reasoner;
import edu.umd.cs.psl.reasoner.function.AtomFunctionVariable;
import edu.umd.cs.psl.reasoner.function.ConstantNumber;
import edu.umd.cs.psl.reasoner.function.ConstraintTerm;
import edu.umd.cs.psl.reasoner.function.FunctionSingleton;
import edu.umd.cs.psl.reasoner.function.FunctionSum;
import edu.umd.cs.psl.reasoner.function.FunctionSummand;
import edu.umd.cs.psl.reasoner.function.FunctionTerm;
import edu.umd.cs.psl.reasoner.function.MaxFunction;
import edu.umd.cs.psl.util.collection.HashList;

/**
 * Performs probabilistic inference over {@link Atom Atoms} based on a set of
 * {@link GroundKernel GroundKernels}.
 * <p>
 * The (unnormalized) probability density function is an exponential model of the
 * following form: P(X) = exp(-sum(w_i * pow(k_i, l_i))), where w_i is the weight of
 * the ith {@link GroundCompatibilityKernel}, k_i is its incompatibility value,
 * and l_i is an exponent with value 1 (linear GroundCompatibilityKernel) or
 * 2 (quadratic GroundCompatibilityKernel).
 * <p>
 * A state X has zero density if any {@link GroundConstraintKernel} is unsatisfied.
 * <p>
 * Uses ADMM optimization method to maximize the density.
 * 
 * @author Stephen Bach <bach@cs.umd.edu>
 */
public class ADMMReasoner implements Reasoner {
	
	private static final Logger log = LoggerFactory.getLogger(ADMMReasoner.class);
	
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
	public static final int MAX_ITER_DEFAULT = 10000;
	
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
	public static final double EPSILON_ABS_DEFAULT = 1e-8;
	
	/**
	 * Key for positive double property. Relative error component of stopping
	 * criteria.
	 */
	public static final String EPSILON_REL_KEY = CONFIG_PREFIX + ".epsilonrel";
	/** Default value for EPSILON_ABS_KEY property */
	public static final double EPSILON_REL_DEFAULT = 1e-3;
	
	/**
	 * Key for positive integer. The number of ADMM iterations after which the
	 * termination criteria will be checked.
	 */
	public static final String STOP_CHECK_KEY = CONFIG_PREFIX + ".stopcheck";
	/** Default value for STOP_CHECK_KEY property */
	public static final int STOP_CHECK_DEFAULT = 1;
	
	/** Key for {@link DistributionType} property. */
	public static final String DISTRIBUTION_KEY = CONFIG_PREFIX + ".distribution";
	/** Default value for DISTRIBUTION_KEY property. */
	public static final DistributionType DISTRIBUTION_DEFAULT = DistributionType.linear;
	
	private final int maxIter;
	/* Sometimes called rho or eta */
	final double stepSize;
	private final DistributionType type;
	
	private final double epsilonRel, epsilonAbs;
	private final int stopCheck;
	private int n;
	
	/** Ground kernels defining the density function */
	Set<GroundKernel> groundKernels;
	/** Ground kernels wrapped to be objective functions for ADMM */
	Vector<ADMMObjectiveTerm> terms;
	/** Ordered list of variables for looking up indices */
	HashList<AtomFunctionVariable> variables;
	/** Consensus vector */
	Vector<Double> z;
	/** Lower bounds on variables */
	Vector<Double> lb;
	/** Upper bounds on variables */
	Vector<Double> ub;
	/** Lists of local variable locations for updating consensus variables */
	Vector<Vector<VariableLocation>> varLocations;
	
	public ADMMReasoner(ConfigBundle config) {
		maxIter = config.getInt(MAX_ITER_KEY, MAX_ITER_DEFAULT);
		stepSize = config.getDouble(STEP_SIZE_KEY, STEP_SIZE_DEFAULT);
		epsilonAbs = config.getDouble(EPSILON_ABS_KEY, EPSILON_ABS_DEFAULT);
		if (epsilonAbs <= 0)
			throw new IllegalArgumentException("Property " + EPSILON_ABS_KEY + " must be positive.");
		epsilonRel = config.getDouble(EPSILON_REL_KEY, EPSILON_REL_DEFAULT);
		if (epsilonRel <= 0)
			throw new IllegalArgumentException("Property " + EPSILON_REL_KEY + " must be positive.");
		stopCheck = config.getInt(STOP_CHECK_KEY, STOP_CHECK_DEFAULT);
		type = (DistributionType) config.getEnum(DISTRIBUTION_KEY, DISTRIBUTION_DEFAULT);
		
		groundKernels = new HashSet<GroundKernel>();
	}
	
	@Override
	public DistributionType getDistributionType() {
		return type;
	}

	@Override
	public void addGroundKernel(GroundKernel gk) {
		groundKernels.add(gk);
	}

	@Override
	public void changedGroundKernel(GroundKernel gk) {
		groundKernels.add(gk);
	}
	
	@Override
	public GroundKernel getGroundKernel(GroundKernel gk) {
		// TODO: make this not a terrible solution
		for (GroundKernel candidate : groundKernels)
			if (gk.equals(candidate))
				return candidate;
		
		return null;
	}

	@Override
	public void removeGroundKernel(GroundKernel gk) {
		groundKernels.remove(gk);
	}

	@Override
	public boolean containsGroundKernel(GroundKernel gk) {
		return groundKernels.contains(gk);
	}

	@Override
	public void optimize() {
		log.debug("Initializing optimization.");
		/* Initializes data structures */
		terms = new Vector<ADMMObjectiveTerm>(groundKernels.size());
		variables = new HashList<AtomFunctionVariable>(groundKernels.size() * 2);
		z = new Vector<Double>(groundKernels.size() * 2);
		lb = new Vector<Double>(groundKernels.size() * 2);
		ub = new Vector<Double>(groundKernels.size() * 2);
		varLocations = new Vector<Vector<VariableLocation>>(groundKernels.size() * 2);
		n = 0;
		
		GroundKernel groundKernel;
		FunctionTerm function, innerFunction, zeroTerm, innerFunctionA, innerFunctionB;
		ADMMObjectiveTerm term;
		
		/* Initializes objective terms from ground kernels */
		for (Iterator<GroundKernel> itr = groundKernels.iterator(); itr.hasNext(); ) {
			groundKernel = itr.next();
			if (groundKernel instanceof GroundCompatibilityKernel) {
				function = ((GroundCompatibilityKernel) groundKernel).getFunctionDefinition();
				/*
				 * If the FunctionTerm is a MaxFunction, ensures that it has two arguments, a linear
				 * function and zero, and constructs the objective term (a hinge loss)
				 */
				if (function instanceof MaxFunction) {
					if (((MaxFunction) function).size() != 2)
						throw new IllegalArgumentException("Max function must have one linear function and 0.0 as arguments.");
					innerFunction = null;
					zeroTerm = null;
					innerFunctionA = ((MaxFunction) function).get(0);
					innerFunctionB = ((MaxFunction) function).get(1);
					
					if (innerFunctionA instanceof ConstantNumber && innerFunctionA.getValue() == 0.0) {
						zeroTerm = innerFunctionA;
						innerFunction = innerFunctionB;
					}
					else if (innerFunctionB instanceof ConstantNumber && innerFunctionB.getValue() == 0.0) {
						zeroTerm = innerFunctionB;
						innerFunction = innerFunctionA;
					}
					
					if (zeroTerm == null)
						throw new IllegalArgumentException("Max function must have one linear function and 0.0 as arguments.");
					
					if (innerFunction instanceof FunctionSum) {
						Hyperplane hp = processHyperplane((FunctionSum) innerFunction);
						if (DistributionType.linear.equals(type)) {
							term = new HingeLossTerm(this, hp.zIndices, hp.coeffs, hp.constant,
									((GroundCompatibilityKernel) groundKernel).getWeight().getWeight());
						}
						else if (DistributionType.quadratic.equals(type)) {
							term = new SquaredHingeLossTerm(this, hp.zIndices, hp.coeffs, hp.constant,
									((GroundCompatibilityKernel) groundKernel).getWeight().getWeight());
						}
						else
							throw new IllegalStateException("Unrecognized DistributionType: " + type);
					}
					else
						throw new IllegalArgumentException("Max function must have one linear function and 0.0 as arguments.");
				}
				/* Else, if it's a FunctionSum, constructs the objective term (a linear loss) */
				else if (function instanceof FunctionSum) {
					Hyperplane hp = processHyperplane((FunctionSum) function);
					if (DistributionType.linear.equals(type)) {
						term = new LinearLossTerm(this, hp.zIndices, hp.coeffs,
								((GroundCompatibilityKernel) groundKernel).getWeight().getWeight());
					}
					else if (DistributionType.quadratic.equals(type)) {
						term = new SquaredLinearLossTerm(this, hp.zIndices, hp.coeffs, 0.0,
								((GroundCompatibilityKernel) groundKernel).getWeight().getWeight());
					}
					else
						throw new IllegalStateException("Unrecognized DistributionType: " + type);
				}
				else
					throw new IllegalArgumentException("Unrecognized function.");
			}
			else if (groundKernel instanceof GroundConstraintKernel) {
				ConstraintTerm constraint = ((GroundConstraintKernel) groundKernel).getConstraintDefinition();
				function = constraint.getFunction();
				if (function instanceof FunctionSum) {
					Hyperplane hp = processHyperplane((FunctionSum) function);
					term = new LinearConstraintTerm(this, hp.zIndices, hp.coeffs,
							constraint.getValue() + hp.constant, constraint.getComparator());
				}
				else
					throw new IllegalArgumentException("Unrecognized function.");
			}
			else
				throw new IllegalStateException("Unsupported ground kernel: " + groundKernel);
			
			registerLocalVariableCopies(term);
			terms.add(term);
		}
		
		log.debug("Performing optimization with {} variables and {} terms.", z.size(), terms.size());
		
		/* Performs inference */
		double primalRes = Double.POSITIVE_INFINITY;
		double dualRes = Double.POSITIVE_INFINITY;
		double epsilonPrimal = 0;
		double epsilonDual = 0;
		double epsilonAbsTerm = Math.sqrt(n) * epsilonAbs;
		double AxNorm = 0.0, BzNorm = 0.0, AyNorm = 0.0;
		boolean check = false;
		int iter = 1;
		while ((primalRes > epsilonPrimal || dualRes > epsilonDual) && iter <= maxIter) {
			check = (iter-1) % stopCheck == 0;
			
			/* Solves each local function */
			for (Iterator<ADMMObjectiveTerm> itr = terms.iterator(); itr.hasNext(); )
				itr.next().updateLagrange().minimize();
			
			/* Updates consensus variables and computes residuals */
			double total, newZ, diff;
			VariableLocation location;
			if (check) {
				primalRes = 0.0;
				dualRes = 0.0;
				AxNorm = 0.0;
				BzNorm = 0.0;
				AyNorm = 0.0;
			}
			for (int i = 0; i < z.size(); i++) {
				total = 0.0;
				/* First pass computes newZ and dual residual */
				for (Iterator<VariableLocation> itr = varLocations.get(i).iterator(); itr.hasNext(); ) {
					location = itr.next();
					total += location.term.x[location.localIndex] + location.term.y[location.localIndex] / stepSize;
					if (check) {
						AxNorm += location.term.x[location.localIndex] * location.term.x[location.localIndex];
						AyNorm += location.term.y[location.localIndex] * location.term.y[location.localIndex];
					}
				}
				newZ = total / varLocations.get(i).size();
				if (newZ < lb.get(i))
					newZ = lb.get(i);
				else if (newZ > ub.get(i))
					newZ = ub.get(i);
				
				if (check) {
					diff = z.get(i) - newZ;
					/* Residual is diff^2 * number of local variables mapped to z element */
					dualRes += diff * diff * varLocations.get(i).size();
					BzNorm += newZ * newZ * varLocations.get(i).size();
				}
				z.set(i, newZ);
				
				/* Second pass computes primal residuals */
				if (check) {
					for (Iterator<VariableLocation> itr = varLocations.get(i).iterator(); itr.hasNext(); ) {
						location = itr.next();
						diff = location.term.x[location.localIndex] - newZ;
						primalRes += diff * diff;
					}
				}
			}

			/* Finishes computing the residuals */
			if (check) {
				primalRes = Math.sqrt(primalRes);
				dualRes = stepSize * Math.sqrt(dualRes);
				
				epsilonPrimal = epsilonAbsTerm + epsilonRel * Math.max(Math.sqrt(AxNorm), Math.sqrt(BzNorm));
				epsilonDual = epsilonAbsTerm + epsilonRel * Math.sqrt(AyNorm);
			}
				
			if ((iter - 1) % 20 == 0) {
				log.debug("Residuals at iter {} -- Primal: {} -- Dual: {}", new Object[] {iter, primalRes, dualRes});
				log.debug("--------- Epsilon primal: {} -- Epsilon dual: {}", epsilonPrimal, epsilonDual);
			}
			
			iter++;
		}
		
		log.debug("Optimization complete.");
		log.debug("Optimization took {} iterations.", iter);
		
		/* Updates variables */
		for (int i = 0; i < variables.size(); i++)
			variables.get(i).setValue(z.get(i));
	}

	@Override
	public Iterable<GroundKernel> getGroundKernels() {
		return Collections.unmodifiableSet(groundKernels);
	}

	@Override
	public Iterable<GroundCompatibilityKernel> getCompatibilityKernels() {
		return Iterables.filter(groundKernels, GroundCompatibilityKernel.class);
	}
	
	public Iterable<GroundConstraintKernel> getConstraintKernels() {
		return Iterables.filter(groundKernels, GroundConstraintKernel.class);
	}

	@Override
	public Iterable<GroundKernel> getGroundKernels(final Kernel k) {
		return Iterables.filter(groundKernels, new com.google.common.base.Predicate<GroundKernel>() {

			@Override
			public boolean apply(GroundKernel gk) {
				return gk.getKernel().equals(k);
			}
			
		});
	}

	@Override
	public double getTotalWeightedIncompatibility() {
		Kernel k;
		double weightedIncompatibility;
		double objective = 0.0;
		for (GroundKernel gk : groundKernels) {
			weightedIncompatibility = gk.getIncompatibility();
			k = gk.getKernel();
			if (k instanceof CompatibilityKernel)
				weightedIncompatibility *= ((CompatibilityKernel) k).getWeight().getWeight();
			objective += weightedIncompatibility;
		}
		return objective;
	}

	@Override
	public int size() {
		return groundKernels.size();
	}

	@Override
	public void close() {
		groundKernels = null;
		terms = null;
		variables = null;
		z = null;
	}
	
	private void registerLocalVariableCopies(ADMMObjectiveTerm term) {
		for (int i = 0; i < term.x.length; i++) {
			VariableLocation varLocation = new VariableLocation(term, i);
			varLocations.get(term.zIndices[i]).add(varLocation);
		}
	}
	
	private Hyperplane processHyperplane(FunctionSum sum) {
		Hyperplane hp = new Hyperplane();
		HashMap<AtomFunctionVariable, Integer> localVarLocations = new HashMap<AtomFunctionVariable, Integer>();
		Vector<Integer> tempZIndices = new Vector<Integer>(sum.size());
		Vector<Double> tempCoeffs = new Vector<Double>(sum.size());
		
		for (Iterator<FunctionSummand> sItr = sum.iterator(); sItr.hasNext(); ) {
			FunctionSummand summand = sItr.next();
			FunctionSingleton singleton = summand.getTerm();
			if (singleton instanceof AtomFunctionVariable && !singleton.isConstant()) {
				int zIndex = variables.indexOf(singleton);
				/*
				 * If this variable has been encountered before in any hyperplane...
				 */
				if (zIndex != -1) {
					/*
					 * Checks if the variable has already been encountered
					 * in THIS hyperplane
					 */
					Integer localIndex = localVarLocations.get(singleton);
					/* If it has, just adds the coefficient... */
					if (localIndex != null) {
						tempCoeffs.set(localIndex, tempCoeffs.get(localIndex) + summand.getCoefficient());
					}
					/* Else, creates a new local variable */
					else {
						tempZIndices.add(zIndex);
						tempCoeffs.add(summand.getCoefficient());
						localVarLocations.put((AtomFunctionVariable) singleton, tempZIndices.size()-1);
						
						/* Increments count of local variables */
						n++;
					}
				}
				/* Else, creates a new global variable and a local variable */
				else {
					/* Creates the global variable */
					variables.add((AtomFunctionVariable) singleton);
					z.add(singleton.getValue());
					lb.add(0.0);
					ub.add(1.0);
					
					/* Creates a list of local variable locations for the new variable */
					varLocations.add(new Vector<ADMMReasoner.VariableLocation>());
					
					/* Creates the local variable */
					tempZIndices.add(z.size()-1);
					tempCoeffs.add(summand.getCoefficient());
					localVarLocations.put((AtomFunctionVariable) singleton, tempZIndices.size()-1);
					
					/* Increments count of local variables */
					n++;
				}
			}
			else if (singleton.isConstant()) {
				/* Subtracts because hyperplane is stored as coeffs^T * x = constant */
				hp.constant -= summand.getValue();
			}
			else
				throw new IllegalArgumentException("Unexpected summand.");
		}

		hp.zIndices = new int[tempZIndices.size()];
		hp.coeffs = new double[tempCoeffs.size()];
		
		for (int i = 0; i < tempZIndices.size(); i++) {
			hp.zIndices[i] = tempZIndices.get(i);
			hp.coeffs[i] = tempCoeffs.get(i);
		}
		
		return hp;
	}
	
	private class Hyperplane {
		int[] zIndices;
		double[] coeffs;
		double constant;
	}
	
	private class VariableLocation {
		private final ADMMObjectiveTerm term;
		private final int localIndex;
		
		private VariableLocation(ADMMObjectiveTerm term, int localIndex) {
			this.term = term;
			this.localIndex = localIndex;
		}
	}

}
