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
package org.linqs.psl.hack;

import java.util.Iterator;

import org.linqs.psl.database.Database;
import org.linqs.psl.database.Queries;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.predicate.Predicate;

import java.util.HashMap;
import java.util.Map;

/**
 * A very quick, very hacky comparator.
 * @HACK: This should not go into core.
*/
public class VeryQuickPredictionComparator {
	
	public static final double DEFAULT_THRESHOLD = 0.5;

	private Map<String, Double> result;
	private Database baseline;
	private double threshold;
	
	int tp;
	int fn;
	int tn;
	int fp;
	double continuousMetricScore;

	public VeryQuickPredictionComparator(Map<String, Double> result) {
		this.result = result;
		baseline = null;
		threshold = DEFAULT_THRESHOLD;
	}
	
	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	public void setBaseline(Database db) {
		this.baseline = db;
	}

	/**
	 * Compares the baseline with te inferred result for a given predicate
	 * DOES NOT check the baseline database for atoms. Only use this if all 
	 * possible predicted atoms are active and unfiltered
	 */
	public VeryQuickPredictionStatistics compare(Predicate p) {
		countResultDBStats(p);
		return new VeryQuickPredictionStatistics(tp, fp, tn, fn, threshold, continuousMetricScore);
	}


	private double accumulate(double difference) {
		return difference * difference;
	}

	/**
	 * Subroutine used by both compare methods for counting statistics from atoms
	 * stored in result database
	 * @param p Predicate to compare against baseline database
	 */
	private void countResultDBStats(Predicate p) {
		tp = 0;
		fn = 0;
		tn = 0;
		fp = 0;
		
		double score = 0.0;
		int total = 0;

		// Fetch all the baseline atoms up front.
		Map<String, Double> baselineAtoms = new HashMap<String, Double>();
		int page = 0;
		while (true) {
			boolean done = true;
			for (GroundAtom atom : Queries.getAllAtoms(baseline, p, page, p.getArity())) {
				baselineAtoms.put(atom.toString(), new Double(atom.getValue()));
				done = false;
				page++;
			}

			if (done) {
				break;
			}
		}

		for (Map.Entry<String, Double> resultEntry : result.entrySet()) {
			// Skip if there is no corresponding baseline atom.
			if (!baselineAtoms.containsKey(resultEntry.getKey())) {
				continue;
			}

			double resultValue = resultEntry.getValue().doubleValue();
			double baselineValue = baselineAtoms.get(resultEntry.getKey()).doubleValue();


			//Continuous comparison statistics
			total++;
			score += accumulate(baselineValue - resultValue);

			boolean actual = (resultValue >= threshold);
			boolean expected = (baselineValue >= threshold);

			if ((actual && expected) || (!actual && !expected)) {
				// True negative
				if (!actual) {
					tn++;
				}
				// True positive
				else {
					tp++;
				}
			}
			// False negative
			else if (!actual) {
				fn++;
			}
			// False positive
			else {
				fp++;
			}
		}

		continuousMetricScore = score / total;
	}
}
